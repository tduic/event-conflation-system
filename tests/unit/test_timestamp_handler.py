"""Unit tests for timestamp handling."""

import pytest
from datetime import datetime, timedelta

from event_conflation.utils.timestamp_handler import (
    normalize_timestamp,
    detect_clock_skew,
    handle_out_of_order,
    calculate_timespan_seconds,
)
from event_conflation.utils.errors import ClockSkewError


def test_normalize_timestamp_iso8601():
    """Test normalizing ISO8601 timestamp."""
    ts = normalize_timestamp("2025-05-01T14:23:05Z")
    assert isinstance(ts, datetime)
    assert ts.year == 2025
    assert ts.month == 5


def test_normalize_timestamp_unix_epoch():
    """Test normalizing Unix epoch timestamp."""
    import time

    now_epoch = time.time()
    ts = normalize_timestamp(now_epoch)

    assert isinstance(ts, datetime)
    # Should be within 1 second of now
    assert abs((datetime.now() - ts).total_seconds()) < 1


def test_normalize_timestamp_datetime():
    """Test normalizing datetime object (passthrough)."""
    dt = datetime.now()
    ts = normalize_timestamp(dt)

    assert ts == dt


def test_normalize_timestamp_invalid():
    """Test that invalid format raises ValueError."""
    with pytest.raises(ValueError):
        normalize_timestamp("not-a-timestamp")


def test_detect_clock_skew_none():
    """Test no clock skew detected for current time."""
    now = datetime.now()
    has_skew = detect_clock_skew(now, max_skew_seconds=300)

    assert has_skew is False


def test_detect_clock_skew_future():
    """Test clock skew detected for future timestamp."""
    future = datetime.now() + timedelta(seconds=400)
    has_skew = detect_clock_skew(future, max_skew_seconds=300)

    assert has_skew is True


def test_detect_clock_skew_past():
    """Test clock skew detected for past timestamp."""
    past = datetime.now() - timedelta(seconds=400)
    has_skew = detect_clock_skew(past, max_skew_seconds=300)

    assert has_skew is True


def test_detect_clock_skew_raise():
    """Test that exception can be raised on skew."""
    future = datetime.now() + timedelta(seconds=400)

    with pytest.raises(ClockSkewError):
        detect_clock_skew(future, max_skew_seconds=300, raise_on_skew=True)


def test_handle_out_of_order():
    """Test handling of out-of-order events."""
    ts1 = datetime.now()
    ts2 = ts1 + timedelta(seconds=10)
    ts3 = ts1 + timedelta(seconds=5)  # Out of order (between ts1 and ts2)

    # Process in order
    result = handle_out_of_order(ts1, ts2)
    assert result == ts2

    # Out of order - should keep ts2
    result = handle_out_of_order(ts2, ts3)
    assert result == ts2

    # New max
    ts4 = ts2 + timedelta(seconds=10)
    result = handle_out_of_order(ts2, ts4)
    assert result == ts4


def test_calculate_timespan_seconds():
    """Test timespan calculation."""
    start = datetime.now()
    end = start + timedelta(seconds=30)

    span = calculate_timespan_seconds(start, end)
    assert abs(span - 30.0) < 0.001  # Allow for floating point precision


def test_calculate_timespan_negative():
    """Test timespan with reversed order (negative)."""
    start = datetime.now()
    end = start - timedelta(seconds=30)

    span = calculate_timespan_seconds(start, end)
    assert abs(span + 30.0) < 0.001
