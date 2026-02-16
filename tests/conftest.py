"""Pytest fixtures and configuration."""

import asyncio
import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from event_conflation.core.models import Event, ConflationConfig
from event_conflation.ingestion.validators import EventValidator


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def default_config(temp_dir):
    """Create a default test configuration."""
    return ConflationConfig(
        idle_interval=0.5,  # Short interval for faster tests
        max_buffer_size=1000,
        state_backend="memory",
        checkpoint_path=str(temp_dir / "checkpoints"),
        output_file_path=str(temp_dir / "output.json"),
        dlq_path=str(temp_dir / "dlq.json"),
        enable_batching=False,  # Disable batching for predictable tests
        log_level="DEBUG",
    )


@pytest.fixture
def sample_event():
    """Create a sample event."""
    return Event(
        id="test-object-1",
        timestamp=datetime.now(),
        payload={"test": "data"}
    )


@pytest.fixture
def sample_events():
    """Create multiple sample events for the same ID."""
    base_time = datetime.now()
    return [
        Event(
            id="test-object-1",
            timestamp=base_time + timedelta(seconds=i),
            payload={"sequence": i}
        )
        for i in range(5)
    ]


@pytest.fixture
def multi_id_events():
    """Create events for multiple IDs."""
    base_time = datetime.now()
    events = []

    for obj_id in range(3):
        for seq in range(3):
            events.append(
                Event(
                    id=f"object-{obj_id}",
                    timestamp=base_time + timedelta(seconds=obj_id * 10 + seq),
                    payload={"sequence": seq}
                )
            )

    return events


@pytest.fixture
def events_file(temp_dir, multi_id_events):
    """Create a JSON events file."""
    file_path = temp_dir / "events.json"

    events_data = [
        {
            "id": e.id,
            "timestamp": e.timestamp.isoformat(),
            "payload": e.payload
        }
        for e in multi_id_events
    ]

    with open(file_path, 'w') as f:
        json.dump(events_data, f)

    return file_path


@pytest.fixture
def validator(temp_dir):
    """Create an event validator."""
    return EventValidator(dlq_path=str(temp_dir / "dlq.json"))


@pytest.fixture
async def mock_emit_callback():
    """Create a mock emit callback that collects emitted records."""
    emitted = []

    async def callback(record):
        emitted.append(record)

    callback.emitted = emitted
    return callback


@pytest.fixture
def out_of_order_events():
    """Create events with out-of-order timestamps."""
    base_time = datetime.now()

    return [
        Event(id="obj-1", timestamp=base_time + timedelta(seconds=2), payload={}),
        Event(id="obj-1", timestamp=base_time + timedelta(seconds=1), payload={}),
        Event(id="obj-1", timestamp=base_time + timedelta(seconds=3), payload={}),
        Event(id="obj-1", timestamp=base_time + timedelta(seconds=0), payload={}),
    ]


@pytest.fixture
def malformed_events():
    """Create malformed event data for testing validation."""
    return [
        {},  # Missing all fields
        {"id": "obj-1"},  # Missing timestamp
        {"timestamp": "2025-01-01T00:00:00Z"},  # Missing id
        {"id": "obj-1", "timestamp": "invalid-timestamp"},  # Invalid timestamp
        {"id": "obj-1", "timestamp": "not-a-date", "payload": "not-a-dict"},  # Multiple errors
    ]
