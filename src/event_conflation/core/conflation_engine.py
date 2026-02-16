"""Conflation engine that orchestrates multiple event processors."""

import asyncio
import logging
from typing import Dict, Callable, Awaitable

from .models import Event, ConflatedRecord, ConflationConfig
from .event_processor import EventProcessor

logger = logging.getLogger(__name__)


class ConflationEngine:
    """
    Orchestrates conflation for multiple event IDs.

    Responsibilities:
    - Route events to appropriate EventProcessor by ID
    - Create/destroy EventProcessors as needed
    - Coordinate graceful shutdown
    - Enforce max buffer size limits
    """

    def __init__(
        self,
        config: ConflationConfig,
        emit_callback: Callable[[ConflatedRecord], Awaitable[None]],
    ):
        """
        Initialize conflation engine.

        Args:
            config: Conflation configuration
            emit_callback: Async callback to invoke when emitting conflated records
        """
        self.config = config
        self.emit_callback = emit_callback
        self.processors: Dict[str, EventProcessor] = {}
        self.lock = asyncio.Lock()
        self._shutdown = False

    async def ingest_event(self, event: Event) -> None:
        """
        Ingest an event and route to appropriate processor.

        Args:
            event: The event to ingest

        Raises:
            RuntimeError: If max buffer size exceeded or engine is shutdown
        """
        if self._shutdown:
            raise RuntimeError("Engine is shutting down")

        async with self.lock:
            # Check buffer size limit
            if (
                event.id not in self.processors
                and len(self.processors) >= self.config.max_buffer_size
            ):
                raise RuntimeError(
                    f"Max buffer size exceeded: {self.config.max_buffer_size} concurrent IDs"
                )

            # Get or create processor for this ID
            if event.id not in self.processors:
                self.processors[event.id] = EventProcessor(
                    event_id=event.id,
                    config=self.config,
                    emit_callback=self._on_record_emitted,
                )
                logger.debug(f"Created new processor for ID: {event.id}")

            processor = self.processors[event.id]

        # Process event (outside lock to allow concurrent processing)
        await processor.process_event(event)

    async def _on_record_emitted(self, record: ConflatedRecord) -> None:
        """
        Internal callback when a processor emits a record.

        Forwards to user-provided callback and cleans up processor.

        Args:
            record: The emitted conflated record
        """
        try:
            # Forward to user callback
            await self.emit_callback(record)

            # Cleanup processor after emission
            async with self.lock:
                if record.id in self.processors:
                    del self.processors[record.id]
                    logger.debug(f"Cleaned up processor for ID: {record.id}")

        except Exception as e:
            logger.error(f"Error in emit callback for {record.id}: {e}")
            raise

    async def shutdown(self) -> None:
        """
        Gracefully shutdown engine.

        Stops accepting new events and shuts down all processors,
        ensuring all pending records are emitted.
        """
        logger.info("Shutting down conflation engine...")
        self._shutdown = True

        async with self.lock:
            processor_ids = list(self.processors.keys())

        # Shutdown all processors concurrently
        if processor_ids:
            logger.info(f"Shutting down {len(processor_ids)} processors...")
            shutdown_tasks = [
                self.processors[pid].shutdown()
                for pid in processor_ids
                if pid in self.processors
            ]
            await asyncio.gather(*shutdown_tasks, return_exceptions=True)

        async with self.lock:
            self.processors.clear()

        logger.info("Conflation engine shutdown complete")

    def get_stats(self) -> Dict[str, int]:
        """Get current engine statistics."""
        return {
            "active_processors": len(self.processors),
            "max_buffer_size": self.config.max_buffer_size,
        }

    async def get_all_states(self) -> Dict[str, dict]:
        """Get states of all processors (for checkpointing)."""
        states = {}
        async with self.lock:
            for event_id, processor in self.processors.items():
                states[event_id] = processor.get_state().model_dump()
        return states

    async def restore_states(self, states: Dict[str, dict]) -> None:
        """Restore processor states from checkpoint."""
        async with self.lock:
            for event_id, state_dict in states.items():
                from .models import ConflationState
                state = ConflationState(**state_dict)

                if event_id not in self.processors:
                    self.processors[event_id] = EventProcessor(
                        event_id=event_id,
                        config=self.config,
                        emit_callback=self._on_record_emitted,
                    )

                await self.processors[event_id].restore_state(state)

        logger.info(f"Restored {len(states)} processor states from checkpoint")
