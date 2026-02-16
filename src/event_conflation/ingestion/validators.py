"""Event validation with DLQ support."""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import aiofiles
from pydantic import ValidationError

from ..core.models import Event

logger = logging.getLogger(__name__)


class EventValidator:
    """
    Validates events and handles malformed data.

    Features:
    - Pydantic-based validation
    - Dead Letter Queue (DLQ) for rejected events
    - Detailed error logging
    """

    def __init__(self, dlq_path: str = "./data/dlq.json"):
        """
        Initialize event validator.

        Args:
            dlq_path: Path to dead letter queue file
        """
        self.dlq_path = Path(dlq_path)
        self.dlq_path.parent.mkdir(parents=True, exist_ok=True)

        self.rejected_count = 0
        self.validated_count = 0

        logger.info(f"Initialized EventValidator with DLQ: {dlq_path}")

    async def validate(self, raw_event: Dict[str, Any]) -> Optional[Event]:
        """
        Validate raw event data.

        Args:
            raw_event: Raw event dictionary

        Returns:
            Validated Event object, or None if validation failed
        """
        try:
            event = Event(**raw_event)
            self.validated_count += 1
            return event

        except ValidationError as e:
            logger.warning(f"Validation failed for event: {e}")
            await self._send_to_dlq(raw_event, str(e))
            self.rejected_count += 1
            return None

        except Exception as e:
            logger.error(f"Unexpected validation error: {e}")
            await self._send_to_dlq(raw_event, f"Unexpected error: {e}")
            self.rejected_count += 1
            return None

    async def _send_to_dlq(self, raw_event: Dict[str, Any], error: str) -> None:
        """
        Send rejected event to dead letter queue.

        Args:
            raw_event: The rejected event data
            error: Error description
        """
        dlq_entry = {
            "timestamp": datetime.now().isoformat(),
            "error": error,
            "event": raw_event,
        }

        try:
            async with aiofiles.open(self.dlq_path, 'a') as f:
                await f.write(json.dumps(dlq_entry) + '\n')

            logger.debug(f"Event sent to DLQ: {self.dlq_path}")

        except Exception as e:
            logger.error(f"Failed to write to DLQ: {e}")

    def get_stats(self) -> dict:
        """Get validation statistics."""
        return {
            "validated": self.validated_count,
            "rejected": self.rejected_count,
            "rejection_rate": (
                self.rejected_count / (self.validated_count + self.rejected_count)
                if (self.validated_count + self.rejected_count) > 0
                else 0.0
            ),
        }


def validate_timestamp(timestamp: Any) -> datetime:
    """
    Validate and normalize timestamp.

    Supports:
    - ISO8601 strings
    - Unix epoch (int/float)
    - datetime objects

    Args:
        timestamp: Timestamp value

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
