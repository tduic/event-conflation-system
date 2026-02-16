"""End-to-end integration tests."""

import asyncio
import json
import pytest
from pathlib import Path
from datetime import datetime, timedelta

from event_conflation.core.conflation_engine import ConflationEngine
from event_conflation.core.models import Event
from event_conflation.ingestion.file_adapter import FileEventAdapter
from event_conflation.ingestion.validators import EventValidator
from event_conflation.output.output_writer import JSONFileWriter


@pytest.mark.asyncio
async def test_single_id_multiple_events(default_config, temp_dir, mock_emit_callback):
    """Test conflation of multiple events for a single ID."""
    engine = ConflationEngine(
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Create events for same ID
    base_time = datetime.now()
    events = [
        Event(id="obj-1", timestamp=base_time + timedelta(seconds=i), payload={})
        for i in range(3)
    ]

    # Ingest events
    for event in events:
        await engine.ingest_event(event)

    # Wait for timer to expire
    await asyncio.sleep(default_config.idle_interval + 0.2)

    # Should have emitted one record
    assert len(mock_emit_callback.emitted) == 1
    record = mock_emit_callback.emitted[0]

    assert record.id == "obj-1"
    assert record.event_count == 3
    assert record.first_event_timestamp == events[0].timestamp
    assert record.last_event_timestamp == events[-1].timestamp

    await engine.shutdown()


@pytest.mark.asyncio
async def test_multiple_ids(default_config, multi_id_events, mock_emit_callback):
    """Test conflation of events for multiple IDs."""
    engine = ConflationEngine(
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Ingest all events
    for event in multi_id_events:
        await engine.ingest_event(event)

    # Wait for timers
    await asyncio.sleep(default_config.idle_interval + 0.2)

    # Should have emitted 3 records (one per unique ID)
    assert len(mock_emit_callback.emitted) == 3

    # Check each record
    records_by_id = {r.id: r for r in mock_emit_callback.emitted}
    assert len(records_by_id) == 3

    for i in range(3):
        obj_id = f"object-{i}"
        assert obj_id in records_by_id
        assert records_by_id[obj_id].event_count == 3

    await engine.shutdown()


@pytest.mark.asyncio
async def test_file_to_output_end_to_end(default_config, temp_dir, events_file):
    """Test complete flow from file input to file output."""
    output_file = temp_dir / "output.json"
    default_config.output_file_path = str(output_file)

    # Create components
    output_writer = JSONFileWriter(
        file_path=str(output_file),
        enable_batching=False,
    )

    async def emit_callback(record):
        await output_writer.write(record)

    engine = ConflationEngine(
        config=default_config,
        emit_callback=emit_callback,
    )

    validator = EventValidator(dlq_path=str(temp_dir / "dlq.json"))
    adapter = FileEventAdapter(validator=validator)

    # Process events from file
    async for event in adapter.read_events(str(events_file)):
        await engine.ingest_event(event)

    # Wait for timers
    await asyncio.sleep(default_config.idle_interval + 0.2)

    await engine.shutdown()
    await output_writer.close()

    # Verify output file
    assert output_file.exists()

    with open(output_file, 'r') as f:
        lines = f.readlines()

    # Should have 3 records (3 unique IDs)
    assert len(lines) == 3

    for line in lines:
        record = json.loads(line)
        assert "id" in record
        assert "first_event_timestamp" in record
        assert "last_event_timestamp" in record
        assert "event_count" in record
        assert record["event_count"] == 3


@pytest.mark.asyncio
async def test_graceful_shutdown_with_pending(default_config, sample_events, mock_emit_callback):
    """Test graceful shutdown emits all pending records."""
    engine = ConflationEngine(
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Ingest events
    for event in sample_events:
        await engine.ingest_event(event)

    # Shutdown immediately (before timer expires)
    await engine.shutdown()

    # Should still emit the pending record
    assert len(mock_emit_callback.emitted) == 1
    assert mock_emit_callback.emitted[0].event_count == len(sample_events)


@pytest.mark.asyncio
async def test_bursty_traffic(default_config, mock_emit_callback):
    """Test handling of bursty traffic (many events in short time)."""
    engine = ConflationEngine(
        config=default_config,
        emit_callback=mock_emit_callback,
    )

    # Generate 100 events across 10 IDs in quick succession
    base_time = datetime.now()
    for i in range(100):
        event = Event(
            id=f"obj-{i % 10}",
            timestamp=base_time + timedelta(milliseconds=i * 10),
            payload={"seq": i}
        )
        await engine.ingest_event(event)

    # Wait for timers
    await asyncio.sleep(default_config.idle_interval + 0.2)

    # Should have 10 records (one per ID)
    assert len(mock_emit_callback.emitted) == 10

    # Each should have ~10 events
    for record in mock_emit_callback.emitted:
        assert record.event_count == 10

    await engine.shutdown()


@pytest.mark.asyncio
async def test_error_handling_with_dlq(default_config, temp_dir):
    """Test that malformed events go to DLQ."""
    dlq_path = temp_dir / "dlq.json"
    validator = EventValidator(dlq_path=str(dlq_path))

    # Create file with mixed valid/invalid events
    events_file = temp_dir / "mixed_events.json"
    with open(events_file, 'w') as f:
        f.write('{"id": "obj-1", "timestamp": "2025-01-01T00:00:00Z"}\n')
        f.write('{"id": "obj-2"}\n')  # Missing timestamp
        f.write('{"timestamp": "2025-01-01T00:00:00Z"}\n')  # Missing ID
        f.write('{"id": "obj-3", "timestamp": "2025-01-01T00:00:00Z"}\n')

    adapter = FileEventAdapter(validator=validator)

    valid_count = 0
    async for event in adapter.read_events(str(events_file)):
        valid_count += 1

    # Should have 2 valid events
    assert valid_count == 2

    # Check DLQ
    assert dlq_path.exists()
    with open(dlq_path, 'r') as f:
        dlq_lines = f.readlines()

    # Should have 2 rejected events
    assert len(dlq_lines) == 2


@pytest.mark.asyncio
async def test_checkpointing(default_config, temp_dir, sample_events):
    """Test state checkpointing and restoration."""
    from event_conflation.state.state_manager import InMemoryStateManager

    checkpoint_path = temp_dir / "checkpoints"
    state_manager = InMemoryStateManager(checkpoint_path=str(checkpoint_path))

    async def emit_callback(record):
        pass

    engine = ConflationEngine(
        config=default_config,
        emit_callback=emit_callback,
    )

    # Ingest events
    for event in sample_events:
        await engine.ingest_event(event)

    # Create checkpoint
    state = await engine.get_all_states()
    await state_manager.checkpoint(state)

    # Verify checkpoint file created
    checkpoint_files = list(checkpoint_path.glob("checkpoint_*.json"))
    assert len(checkpoint_files) > 0

    # Load checkpoint
    loaded_state = await state_manager.load_latest_checkpoint()
    assert sample_events[0].id in loaded_state

    await engine.shutdown()
