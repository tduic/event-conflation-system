"""File-based event ingestion adapter."""

import json
import logging
from pathlib import Path
from typing import AsyncGenerator, List, Dict, Any

import aiofiles

from ..core.models import Event
from .validators import EventValidator

logger = logging.getLogger(__name__)


class FileEventAdapter:
    """
    Reads events from JSON file.

    Supports:
    - JSONL format (one event per line)
    - JSON array format
    - Async file reading
    - Event validation with DLQ
    """

    def __init__(self, validator: EventValidator):
        """
        Initialize file event adapter.

        Args:
            validator: Event validator instance
        """
        self.validator = validator

    async def read_events(self, file_path: str) -> AsyncGenerator[Event, None]:
        """
        Read events from file asynchronously.

        Supports both JSONL and JSON array formats.

        Args:
            file_path: Path to events file

        Yields:
            Validated Event objects
        """
        file = Path(file_path)

        if not file.exists():
            raise FileNotFoundError(f"Events file not found: {file_path}")

        logger.info(f"Reading events from {file_path}")

        try:
            async with aiofiles.open(file, 'r') as f:
                content = await f.read()

            # Try to parse as JSON array first
            try:
                events_data = json.loads(content)
                if isinstance(events_data, list):
                    logger.debug(f"Detected JSON array format with {len(events_data)} events")
                    for raw_event in events_data:
                        event = await self.validator.validate(raw_event)
                        if event:
                            yield event
                    return
            except json.JSONDecodeError:
                pass

            # Parse as JSONL (one JSON object per line)
            lines = content.strip().split('\n')
            logger.debug(f"Detected JSONL format with {len(lines)} lines")

            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    raw_event = json.loads(line)
                    event = await self.validator.validate(raw_event)
                    if event:
                        yield event

                except json.JSONDecodeError as e:
                    logger.warning(f"Invalid JSON on line {line_num}: {e}")
                    await self.validator._send_to_dlq(
                        {"raw_line": line},
                        f"JSON parse error on line {line_num}: {e}"
                    )

        except Exception as e:
            logger.error(f"Failed to read events from {file_path}: {e}")
            raise

    async def read_all_events(self, file_path: str) -> List[Event]:
        """
        Read all events from file into memory.

        Args:
            file_path: Path to events file

        Returns:
            List of validated events
        """
        events = []
        async for event in self.read_events(file_path):
            events.append(event)

        logger.info(f"Read {len(events)} valid events from {file_path}")
        return events


class EventGenerator:
    """
    Generates events programmatically for testing.

    Useful for:
    - Load testing
    - Benchmarking
    - Integration tests
    """

    @staticmethod
    async def generate_events(
        num_events: int,
        num_unique_ids: int,
        output_path: str = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate test events.

        Args:
            num_events: Total number of events to generate
            num_unique_ids: Number of unique event IDs
            output_path: Optional path to write events to file

        Returns:
            List of event dictionaries
        """
        from datetime import datetime, timedelta
        import random

        events = []
        base_time = datetime.now()

        for i in range(num_events):
            event = {
                "id": f"object-{i % num_unique_ids}",
                "timestamp": (base_time + timedelta(seconds=i * 0.1)).isoformat(),
                "payload": {
                    "sequence": i,
                    "random_value": random.randint(1, 1000),
                }
            }
            events.append(event)

        logger.info(f"Generated {num_events} events with {num_unique_ids} unique IDs")

        # Optionally write to file
        if output_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(output_file, 'w') as f:
                await f.write(json.dumps(events, indent=2))

            logger.info(f"Wrote events to {output_path}")

        return events
