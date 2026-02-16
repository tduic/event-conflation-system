"""Unit tests for event validation."""

import pytest
import json
from pathlib import Path

from event_conflation.ingestion.validators import EventValidator, validate_timestamp
from datetime import datetime


@pytest.mark.asyncio
async def test_valid_event(validator):
    """Test validation of a valid event."""
    raw_event = {
        "id": "obj-1",
        "timestamp": "2025-05-01T14:23:05Z",
        "payload": {"test": "data"}
    }

    event = await validator.validate(raw_event)

    assert event is not None
    assert event.id == "obj-1"
    assert event.payload == {"test": "data"}
    assert validator.validated_count == 1
    assert validator.rejected_count == 0


@pytest.mark.asyncio
async def test_missing_id(validator, temp_dir):
    """Test validation of event missing ID."""
    raw_event = {
        "timestamp": "2025-05-01T14:23:05Z",
        "payload": {}
    }

    event = await validator.validate(raw_event)

    assert event is None
    assert validator.rejected_count == 1

    # Check DLQ
    dlq_file = Path(validator.dlq_path)
    assert dlq_file.exists()

    with open(dlq_file, 'r') as f:
        dlq_entry = json.loads(f.readline())
        assert "id" in dlq_entry["error"].lower()


@pytest.mark.asyncio
async def test_missing_timestamp(validator):
    """Test validation of event missing timestamp."""
    raw_event = {
        "id": "obj-1",
        "payload": {}
    }

    event = await validator.validate(raw_event)

    assert event is None
    assert validator.rejected_count == 1


@pytest.mark.asyncio
async def test_invalid_timestamp_format(validator):
    """Test validation of event with invalid timestamp."""
    raw_event = {
        "id": "obj-1",
        "timestamp": "not-a-valid-timestamp",
        "payload": {}
    }

    event = await validator.validate(raw_event)

    assert event is None
    assert validator.rejected_count == 1


@pytest.mark.asyncio
async def test_multiple_validations(validator, malformed_events):
    """Test validation statistics with multiple events."""
    for raw_event in malformed_events:
        await validator.validate(raw_event)

    stats = validator.get_stats()

    assert stats['validated'] == 0
    assert stats['rejected'] == len(malformed_events)
    assert stats['rejection_rate'] == 1.0


@pytest.mark.asyncio
async def test_unix_epoch_timestamp(validator):
    """Test validation with Unix epoch timestamp."""
    import time

    raw_event = {
        "id": "obj-1",
        "timestamp": int(time.time()),
        "payload": {}
    }

    event = await validator.validate(raw_event)

    assert event is not None
    assert isinstance(event.timestamp, datetime)


@pytest.mark.asyncio
async def test_iso8601_timestamp_variants(validator):
    """Test various ISO8601 timestamp formats."""
    variants = [
        "2025-05-01T14:23:05Z",
        "2025-05-01T14:23:05+00:00",
        "2025-05-01T14:23:05",
    ]

    for ts in variants:
        raw_event = {
            "id": "obj-1",
            "timestamp": ts,
            "payload": {}
        }

        event = await validator.validate(raw_event)
        assert event is not None, f"Failed to validate timestamp: {ts}"


def test_validate_timestamp_function():
    """Test the standalone validate_timestamp function."""
    # Test ISO8601
    ts = validate_timestamp("2025-05-01T14:23:05Z")
    assert isinstance(ts, datetime)

    # Test Unix epoch
    import time
    now = time.time()
    ts = validate_timestamp(now)
    assert isinstance(ts, datetime)

    # Test datetime object
    dt = datetime.now()
    ts = validate_timestamp(dt)
    assert ts == dt

    # Test invalid
    with pytest.raises(ValueError):
        validate_timestamp("invalid")
