"""Output writers for conflated records."""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

import aiofiles

from ..core.models import ConflatedRecord

logger = logging.getLogger(__name__)


class OutputWriter(ABC):
    """Abstract interface for output writers."""

    @abstractmethod
    async def write(self, record: ConflatedRecord) -> None:
        """Write a single conflated record."""
        pass

    @abstractmethod
    async def flush(self) -> None:
        """Flush any buffered records."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close the writer and release resources."""
        pass


class JSONFileWriter(OutputWriter):
    """
    JSON file writer with optional batching.

    Writes records in DynamoDB-compatible JSON format:
    - One JSON object per line (JSONL format)
    - Batching for improved I/O performance
    - Async file operations
    """

    def __init__(
        self,
        file_path: str,
        enable_batching: bool = True,
        batch_size: int = 100,
    ):
        """
        Initialize JSON file writer.

        Args:
            file_path: Path to output file
            enable_batching: Enable batching for better performance
            batch_size: Number of records to batch before writing
        """
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

        self.enable_batching = enable_batching
        self.batch_size = batch_size

        self.buffer: List[ConflatedRecord] = []
        self.lock = asyncio.Lock()
        self.total_written = 0

        logger.info(
            f"Initialized JSONFileWriter: {file_path}, "
            f"batching={'enabled' if enable_batching else 'disabled'}, "
            f"batch_size={batch_size}"
        )

    async def write(self, record: ConflatedRecord) -> None:
        """
        Write a conflated record.

        If batching is enabled, buffers until batch_size is reached.

        Args:
            record: The record to write
        """
        async with self.lock:
            self.buffer.append(record)

            # Write if batch is full or batching is disabled
            if not self.enable_batching or len(self.buffer) >= self.batch_size:
                await self._flush_buffer()

    async def flush(self) -> None:
        """Flush any buffered records to disk."""
        async with self.lock:
            if self.buffer:
                await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        """Internal method to write buffer to file (must hold lock)."""
        if not self.buffer:
            return

        try:
            # Convert records to JSON lines
            lines = []
            for record in self.buffer:
                # Convert to dict with ISO format timestamps
                record_dict = {
                    "id": record.id,
                    "first_event_timestamp": record.first_event_timestamp.isoformat(),
                    "last_event_timestamp": record.last_event_timestamp.isoformat(),
                    "event_count": record.event_count,
                }
                lines.append(json.dumps(record_dict))

            # Append to file
            async with aiofiles.open(self.file_path, 'a') as f:
                await f.write('\n'.join(lines) + '\n')

            count = len(self.buffer)
            self.total_written += count
            logger.debug(f"Flushed {count} records to {self.file_path} (total: {self.total_written})")

            # Clear buffer
            self.buffer.clear()

        except Exception as e:
            logger.error(f"Failed to write to {self.file_path}: {e}")
            raise

    async def close(self) -> None:
        """Close writer and flush remaining records."""
        await self.flush()
        logger.info(f"JSONFileWriter closed. Total records written: {self.total_written}")

    def get_stats(self) -> dict:
        """Get writer statistics."""
        return {
            "total_written": self.total_written,
            "buffered": len(self.buffer),
            "file_path": str(self.file_path),
        }


class ConsoleWriter(OutputWriter):
    """
    Console writer for debugging.

    Writes records to stdout in human-readable format.
    """

    def __init__(self):
        """Initialize console writer."""
        self.total_written = 0
        logger.info("Initialized ConsoleWriter")

    async def write(self, record: ConflatedRecord) -> None:
        """Write record to console."""
        print(f"[CONFLATED] ID: {record.id}")
        print(f"  First: {record.first_event_timestamp.isoformat()}")
        print(f"  Last:  {record.last_event_timestamp.isoformat()}")
        print(f"  Count: {record.event_count}")
        print()

        self.total_written += 1

    async def flush(self) -> None:
        """Flush (no-op for console)."""
        pass

    async def close(self) -> None:
        """Close writer."""
        logger.info(f"ConsoleWriter closed. Total records written: {self.total_written}")


def create_output_writer(
    writer_type: str,
    file_path: str = None,
    enable_batching: bool = True,
    batch_size: int = 100,
) -> OutputWriter:
    """
    Factory function to create output writer.

    Args:
        writer_type: Type of writer ('json' or 'console')
        file_path: Path for file-based writers
        enable_batching: Enable batching for better performance
        batch_size: Batch size for batched writers

    Returns:
        OutputWriter instance
    """
    if writer_type == "json":
        if not file_path:
            raise ValueError("file_path required for json writer")
        return JSONFileWriter(
            file_path=file_path,
            enable_batching=enable_batching,
            batch_size=batch_size,
        )
    elif writer_type == "console":
        return ConsoleWriter()
    else:
        raise ValueError(f"Unknown writer type: {writer_type}")
