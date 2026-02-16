"""Unit tests for EventProcessor."""

import asyncio
import pytest
from datetime import datetime, timedelta

from event_conflation.core.event_processor import EventProcessor
from event_conflation.core.models import Event, ConflationConfig


@pytest.mark.asyncio
async def test_single_event_emission(default_config, sample_event, mock_emit_callback):
    """Test that a single event is emitted after idle interval."""
    processor = EventProcessor(
        event_id=sample_event.id,
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Process event
    await processor.process_event(sample_event)

    # Should not emit immediately
    assert len(mock_emit_callback.emitted) == 0

    # Wait for idle interval + buffer
    await asyncio.sleep(default_config.idle_interval + 0.1)

    # Should have emitted
    assert len(mock_emit_callback.emitted) == 1
    record = mock_emit_callback.emitted[0]

    assert record.id == sample_event.id
    assert record.event_count == 1
    assert record.first_event_timestamp == sample_event.timestamp
    assert record.last_event_timestamp == sample_event.timestamp


@pytest.mark.asyncio
async def test_timer_reset(default_config, sample_events, mock_emit_callback):
    """Test that timer resets when new events arrive."""
    processor = EventProcessor(
        event_id=sample_events[0].id,
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Process events with small delays (less than idle interval)
    for event in sample_events:
        await processor.process_event(event)
        await asyncio.sleep(0.1)  # Less than idle_interval

    # Should not have emitted yet
    assert len(mock_emit_callback.emitted) == 0

    # Wait for idle interval after last event
    await asyncio.sleep(default_config.idle_interval + 0.1)

    # Should have emitted once with all events
    assert len(mock_emit_callback.emitted) == 1
    record = mock_emit_callback.emitted[0]

    assert record.event_count == len(sample_events)
    assert record.first_event_timestamp == sample_events[0].timestamp
    assert record.last_event_timestamp == sample_events[-1].timestamp


@pytest.mark.asyncio
async def test_multiple_emissions(default_config, sample_event, mock_emit_callback):
    """Test multiple emission cycles."""
    processor = EventProcessor(
        event_id=sample_event.id,
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # First cycle
    await processor.process_event(sample_event)
    await asyncio.sleep(default_config.idle_interval + 0.1)

    assert len(mock_emit_callback.emitted) == 1
    assert mock_emit_callback.emitted[0].event_count == 1

    # Second cycle
    await processor.process_event(sample_event)
    await asyncio.sleep(default_config.idle_interval + 0.1)

    assert len(mock_emit_callback.emitted) == 2
    assert mock_emit_callback.emitted[1].event_count == 1


@pytest.mark.asyncio
async def test_out_of_order_timestamps(default_config, out_of_order_events, mock_emit_callback):
    """Test that out-of-order events use max timestamp."""
    processor = EventProcessor(
        event_id=out_of_order_events[0].id,
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Process out-of-order events
    for event in out_of_order_events:
        await processor.process_event(event)

    await asyncio.sleep(default_config.idle_interval + 0.1)

    assert len(mock_emit_callback.emitted) == 1
    record = mock_emit_callback.emitted[0]

    # Should use earliest timestamp as first
    assert record.first_event_timestamp == out_of_order_events[0].timestamp

    # Should use latest timestamp as last (max of all)
    expected_last = max(e.timestamp for e in out_of_order_events)
    assert record.last_event_timestamp == expected_last


@pytest.mark.asyncio
async def test_state_tracking(default_config, sample_events, mock_emit_callback):
    """Test that state is correctly tracked."""
    processor = EventProcessor(
        event_id=sample_events[0].id,
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Process events
    for event in sample_events:
        await processor.process_event(event)

    # Check state before emission
    state = processor.get_state()
    assert state.event_count == len(sample_events)
    assert state.first_timestamp == sample_events[0].timestamp
    assert state.last_timestamp == sample_events[-1].timestamp

    # Wait for emission
    await asyncio.sleep(default_config.idle_interval + 0.1)

    # State should be reset after emission
    state = processor.get_state()
    assert state.event_count == 0


@pytest.mark.asyncio
async def test_graceful_shutdown(default_config, sample_events, mock_emit_callback):
    """Test graceful shutdown emits pending records."""
    processor = EventProcessor(
        event_id=sample_events[0].id,
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Process events but don't wait for timer
    for event in sample_events:
        await processor.process_event(event)

    # Shutdown immediately
    await processor.shutdown()

    # Should have emitted despite timer not expiring
    assert len(mock_emit_callback.emitted) == 1
    assert mock_emit_callback.emitted[0].event_count == len(sample_events)


@pytest.mark.asyncio
async def test_clock_skew_warning(default_config, mock_emit_callback, caplog):
    """Test that clock skew is detected and logged."""
    import logging
    caplog.set_level(logging.WARNING)

    processor = EventProcessor(
        event_id="test-obj",
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Create event with timestamp far in the future
    future_event = Event(
        id="test-obj",
        timestamp=datetime.now() + timedelta(seconds=1000),  # Way beyond max_clock_skew
        payload={}
    )

    await processor.process_event(future_event)

    # Should log warning about clock skew
    assert any("Clock skew detected" in record.message for record in caplog.records)


@pytest.mark.asyncio
async def test_state_restore(default_config, sample_events, mock_emit_callback):
    """Test that processor can restore state."""
    from event_conflation.core.models import ConflationState

    processor = EventProcessor(
        event_id=sample_events[0].id,
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Create a state to restore
    restore_state = ConflationState(
        id=sample_events[0].id,
        first_timestamp=sample_events[0].timestamp,
        last_timestamp=sample_events[2].timestamp,
        event_count=3,
        last_update=datetime.now(),
    )

    # Restore state
    await processor.restore_state(restore_state)

    # Should restart timer and eventually emit
    await asyncio.sleep(default_config.idle_interval + 0.1)

    assert len(mock_emit_callback.emitted) == 1
    record = mock_emit_callback.emitted[0]
    assert record.event_count == 3
