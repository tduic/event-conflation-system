"""Per-ID event processor with timer management."""

import asyncio
import logging
from datetime import datetime
from typing import Callable, Optional, Awaitable

from .models import Event, ConflatedRecord, ConflationState, ConflationConfig

logger = logging.getLogger(__name__)


class EventProcessor:
    """
    Manages conflation for a single event ID.

    Each processor maintains:
    - A cancellable timer task
    - State (first_timestamp, last_timestamp, event_count)
    - Thread-safe state updates via asyncio.Lock
    """

    def __init__(
        self,
        event_id: str,
        config: ConflationConfig,
        emit_callback: Callable[[ConflatedRecord], Awaitable[None]],
    ):
        """
        Initialize event processor for a specific ID.

        Args:
            event_id: The unique event ID this processor handles
            config: Conflation configuration
            emit_callback: Async callback to invoke when emitting conflated record
        """
        self.event_id = event_id
        self.config = config
        self.emit_callback = emit_callback

        # State
        self.state = ConflationState(id=event_id)
        self.lock = asyncio.Lock()
        self.timer_task: Optional[asyncio.Task] = None
        self._shutdown = False

    async def process_event(self, event: Event) -> None:
        """
        Process an incoming event.

        Algorithm:
        1. Acquire lock
        2. Cancel existing timer
        3. Update state (first_ts, last_ts, count)
        4. Start new timer
        5. Release lock

        Args:
            event: The event to process
        """
        if self._shutdown:
            logger.warning(f"Processor {self.event_id} is shutting down, ignoring event")
            return

        async with self.lock:
            # Cancel existing timer
            if self.timer_task and not self.timer_task.done():
                self.timer_task.cancel()
                try:
                    await self.timer_task
                except asyncio.CancelledError:
                    pass

            # Update state
            if self.state.first_timestamp is None:
                self.state.first_timestamp = event.timestamp
                self.state.last_timestamp = event.timestamp
            else:
                # Handle out-of-order events: use max timestamp
                self.state.last_timestamp = max(
                    self.state.last_timestamp,
                    event.timestamp
                )

            self.state.event_count += 1
            self.state.last_update = datetime.now()

            # Check for clock skew
            clock_skew = abs((datetime.now() - event.timestamp).total_seconds())
            if clock_skew > self.config.max_clock_skew:
                logger.warning(
                    f"Clock skew detected for event {self.event_id}: "
                    f"{clock_skew:.1f}s (max: {self.config.max_clock_skew}s)"
                )

            # Start new timer
            self.timer_task = asyncio.create_task(self._wait_and_emit())

            logger.debug(
                f"Event processed for {self.event_id}: "
                f"count={self.state.event_count}, "
                f"timer reset to {self.config.idle_interval}s"
            )

    async def _wait_and_emit(self) -> None:
        """
        Wait for idle interval, then emit conflated record.

        This method is run as an asyncio task and can be cancelled
        if a new event arrives before the timer expires.
        """
        try:
            await asyncio.sleep(self.config.idle_interval)

            # Timer expired without cancellation - emit record
            async with self.lock:
                await self._emit_conflated_record()

        except asyncio.CancelledError:
            # Timer was cancelled due to new event - this is normal
            logger.debug(f"Timer cancelled for {self.event_id} (new event arrived)")
            raise

    async def _emit_conflated_record(self) -> None:
        """Emit the conflated record via callback. Must hold lock."""
        if self.state.event_count == 0:
            logger.warning(f"Attempted to emit empty record for {self.event_id}")
            return

        record = ConflatedRecord(
            id=self.event_id,
            first_event_timestamp=self.state.first_timestamp,
            last_event_timestamp=self.state.last_timestamp,
            event_count=self.state.event_count,
        )

        logger.info(
            f"Emitting conflated record for {self.event_id}: "
            f"{self.state.event_count} events, "
            f"timespan: {self.state.first_timestamp} -> {self.state.last_timestamp}"
        )

        try:
            await self.emit_callback(record)
        except Exception as e:
            logger.error(f"Failed to emit record for {self.event_id}: {e}")
            raise

        # Reset state after successful emission
        self.state = ConflationState(id=self.event_id)

    async def shutdown(self) -> None:
        """
        Gracefully shutdown processor.

        Cancels timer and emits any pending conflated record.
        """
        self._shutdown = True

        async with self.lock:
            # Cancel timer
            if self.timer_task and not self.timer_task.done():
                self.timer_task.cancel()
                try:
                    await self.timer_task
                except asyncio.CancelledError:
                    pass

            # Emit pending record if any
            if self.state.event_count > 0:
                logger.info(f"Shutdown: emitting pending record for {self.event_id}")
                await self._emit_conflated_record()

    def get_state(self) -> ConflationState:
        """Get current state (for checkpointing)."""
        return self.state.model_copy(deep=True)

    async def restore_state(self, state: ConflationState) -> None:
        """Restore state from checkpoint."""
        async with self.lock:
            self.state = state
            # Restart timer if there's pending data
            if self.state.event_count > 0:
                if self.timer_task and not self.timer_task.done():
                    self.timer_task.cancel()
                self.timer_task = asyncio.create_task(self._wait_and_emit())
                logger.info(f"Restored state for {self.event_id}: {self.state.event_count} events pending")
