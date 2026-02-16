"""Main entry point for event conflation system."""

import asyncio
import logging
import signal
import sys
from pathlib import Path
from typing import Optional

from .config.config_manager import ConfigManager
from .core.conflation_engine import ConflationEngine
from .core.models import ConflationConfig, ConflatedRecord
from .ingestion.file_adapter import FileEventAdapter
from .ingestion.validators import EventValidator
from .output.output_writer import create_output_writer, OutputWriter
from .state.state_manager import create_state_manager, StateManager


# Configure logging
def setup_logging(level: str = "INFO", log_format: str = "text"):
    """Setup logging configuration."""
    if log_format == "json":
        import json

        class JSONFormatter(logging.Formatter):
            def format(self, record):
                log_data = {
                    "timestamp": self.formatTime(record),
                    "level": record.levelname,
                    "logger": record.name,
                    "message": record.getMessage(),
                }
                if record.exc_info:
                    log_data["exception"] = self.formatException(record.exc_info)
                return json.dumps(log_data)

        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        )

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        handlers=[handler]
    )


class ConflationService:
    """
    Main service orchestrating the event conflation system.

    Responsibilities:
    - Initialize all components
    - Run the event processing loop
    - Handle graceful shutdown
    - Periodic checkpointing
    """

    def __init__(self, config: ConflationConfig):
        """
        Initialize conflation service.

        Args:
            config: Conflation configuration
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Initialize components
        self.output_writer: Optional[OutputWriter] = None
        self.state_manager: Optional[StateManager] = None
        self.engine: Optional[ConflationEngine] = None
        self.validator: Optional[EventValidator] = None

        self._shutdown_event = asyncio.Event()
        self._checkpoint_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Initialize all components."""
        self.logger.info("Initializing conflation service...")

        # Create output writer
        self.output_writer = create_output_writer(
            writer_type=self.config.output_writer,
            file_path=self.config.output_file_path,
            enable_batching=self.config.enable_batching,
            batch_size=self.config.batch_size,
        )

        # Create state manager
        self.state_manager = create_state_manager(
            backend=self.config.state_backend,
            checkpoint_path=self.config.checkpoint_path,
            state_file_path=self.config.state_file_path,
        )

        # Create conflation engine
        self.engine = ConflationEngine(
            config=self.config,
            emit_callback=self._on_record_emitted,
        )

        # Create validator
        self.validator = EventValidator(dlq_path=self.config.dlq_path)

        # Try to restore from checkpoint
        try:
            checkpoint_state = await self.state_manager.load_latest_checkpoint()
            if checkpoint_state:
                await self.engine.restore_states(checkpoint_state)
                self.logger.info("Restored state from checkpoint")
        except Exception as e:
            self.logger.warning(f"Could not restore from checkpoint: {e}")

        # Start periodic checkpointing
        self._checkpoint_task = asyncio.create_task(self._periodic_checkpoint())

        self.logger.info("Conflation service initialized")

    async def _on_record_emitted(self, record: ConflatedRecord):
        """Callback when engine emits a conflated record."""
        await self.output_writer.write(record)

    async def _periodic_checkpoint(self):
        """Periodically checkpoint the engine state."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.checkpoint_interval)

                if not self._shutdown_event.is_set():
                    state = await self.engine.get_all_states()
                    await self.state_manager.checkpoint(state)
                    self.logger.debug("Periodic checkpoint completed")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Checkpoint failed: {e}")

    async def process_file(self, events_file: str):
        """
        Process events from a file.

        Args:
            events_file: Path to events file
        """
        self.logger.info(f"Processing events from {events_file}")

        adapter = FileEventAdapter(validator=self.validator)

        event_count = 0
        try:
            async for event in adapter.read_events(events_file):
                if self._shutdown_event.is_set():
                    self.logger.info("Shutdown signal received, stopping ingestion")
                    break

                await self.engine.ingest_event(event)
                event_count += 1

                if event_count % 1000 == 0:
                    self.logger.info(f"Processed {event_count} events...")

        except Exception as e:
            self.logger.error(f"Error processing events: {e}")
            raise

        self.logger.info(f"Ingestion complete: {event_count} events processed")

        # Log validation stats
        stats = self.validator.get_stats()
        self.logger.info(
            f"Validation stats: {stats['validated']} validated, "
            f"{stats['rejected']} rejected "
            f"({stats['rejection_rate']:.2%} rejection rate)"
        )

    async def shutdown(self):
        """Gracefully shutdown the service."""
        self.logger.info("Shutting down conflation service...")
        self._shutdown_event.set()

        # Stop checkpoint task
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
            try:
                await self._checkpoint_task
            except asyncio.CancelledError:
                pass

        # Shutdown engine (emits all pending records)
        if self.engine:
            await self.engine.shutdown()

        # Final checkpoint
        if self.engine and self.state_manager:
            try:
                state = await self.engine.get_all_states()
                await self.state_manager.checkpoint(state)
                self.logger.info("Final checkpoint completed")
            except Exception as e:
                self.logger.error(f"Final checkpoint failed: {e}")

        # Close output writer
        if self.output_writer:
            await self.output_writer.close()

        self.logger.info("Conflation service shutdown complete")


async def main(config_path: Optional[str] = None, events_file: Optional[str] = None):
    """
    Main async entry point.

    Args:
        config_path: Path to config file (optional)
        events_file: Path to events file (optional)
    """
    # Load configuration
    if config_path:
        config = ConfigManager.load_from_file(config_path)
    else:
        config = ConfigManager.load_default()

    # Setup logging
    setup_logging(level=config.log_level, log_format=config.log_format)

    # Create and initialize service
    service = ConflationService(config)

    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_event_loop()

    def signal_handler(sig):
        logging.getLogger(__name__).info(f"Received signal {sig}, initiating shutdown")
        asyncio.create_task(service.shutdown())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))

    try:
        await service.initialize()

        if events_file:
            await service.process_file(events_file)

            # Wait for all timers to expire (idle_interval + buffer)
            logging.getLogger(__name__).info(
                f"Waiting {config.idle_interval + 2}s for timers to expire..."
            )
            await asyncio.sleep(config.idle_interval + 2)
        else:
            # Run indefinitely (useful for streaming mode in future)
            logging.getLogger(__name__).info("No events file provided, running in idle mode")
            await service._shutdown_event.wait()

    except Exception as e:
        logging.getLogger(__name__).error(f"Fatal error: {e}", exc_info=True)
        raise
    finally:
        await service.shutdown()


def cli():
    """Command-line interface."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Event Conflation System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--config',
        '-c',
        help='Path to configuration file (YAML)',
        default=None,
    )
    parser.add_argument(
        '--events',
        '-e',
        help='Path to events file (JSON/JSONL)',
        default=None,
    )
    parser.add_argument(
        '--idle-interval',
        type=float,
        help='Override idle interval (seconds)',
        default=None,
    )

    args = parser.parse_args()

    # Load config
    if args.config:
        config = ConfigManager.load_from_file(args.config)
    else:
        config = ConfigManager.load_default()

    # Apply CLI overrides
    if args.idle_interval is not None:
        config.idle_interval = args.idle_interval

    # Setup logging before running
    setup_logging(level=config.log_level, log_format=config.log_format)

    # Run service
    try:
        asyncio.run(main(config_path=args.config, events_file=args.events))
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Interrupted by user")
        sys.exit(0)


if __name__ == '__main__':
    cli()
