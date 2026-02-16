"""Timestamp normalization and clock skew detection."""

import logging
from datetime import datetime
from typing import Union

from .errors import ClockSkewError

logger = logging.getLogger(__name__)


def normalize_timestamp(timestamp: Union[str, int, float, datetime]) -> datetime:
    """
    Normalize timestamp to datetime object.

    Supports:
    - ISO8601 strings (e.g., "2025-05-01T14:23:05Z")
    - Unix epoch (int/float)
    - datetime objects

    Args:
        timestamp: Timestamp in various formats

    Returns:
        datetime object

    Raises:
        ValueError: If timestamp format is invalid
    """
    if isinstance(timestamp, datetime):
        return timestamp

    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp)

    if isinstance(timestamp, str):
        # Try ISO8601 format
        try:
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except ValueError:
            pass

        # Try Unix epoch as string
        try:
            return datetime.fromtimestamp(float(timestamp))
        except (ValueError, TypeError):
            pass

    raise ValueError(f"Invalid timestamp format: {timestamp}")


def detect_clock_skew(
    event_timestamp: datetime,
    max_skew_seconds: int = 300,
    raise_on_skew: bool = False,
) -> bool:
    """
    Detect if event timestamp has significant clock skew.

    Args:
        event_timestamp: The event's timestamp
        max_skew_seconds: Maximum allowed skew in seconds (default: 5 minutes)
        raise_on_skew: If True, raise exception on detection

    Returns:
        True if clock skew detected, False otherwise

    Raises:
        ClockSkewError: If raise_on_skew=True and skew detected
    """
    now = datetime.now()
    skew = abs((event_timestamp - now).total_seconds())

    if skew > max_skew_seconds:
        message = (
            f"Clock skew detected: event timestamp {event_timestamp.isoformat()} "
            f"differs from current time by {skew:.1f}s (max: {max_skew_seconds}s)"
        )

        if raise_on_skew:
            raise ClockSkewError(message)

        logger.warning(message)
        return True

    return False


def handle_out_of_order(current_last: datetime, new_timestamp: datetime) -> datetime:
    """
    Handle out-of-order events by taking maximum timestamp.

    Args:
        current_last: Current last timestamp
        new_timestamp: New event timestamp

    Returns:
        Maximum of the two timestamps
    """
    result = max(current_last, new_timestamp)

    if new_timestamp < current_last:
        logger.debug(
            f"Out-of-order event detected: "
            f"new={new_timestamp.isoformat()}, "
            f"current={current_last.isoformat()}, "
            f"using max={result.isoformat()}"
        )

    return result


def calculate_timespan_seconds(start: datetime, end: datetime) -> float:
    """
    Calculate timespan in seconds between two timestamps.

    Args:
        start: Start timestamp
        end: End timestamp

    Returns:
        Timespan in seconds
    """
    return (end - start).total_seconds()
