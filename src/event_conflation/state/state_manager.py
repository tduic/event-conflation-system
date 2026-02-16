"""State management with pluggable backends."""

import json
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import aiofiles

logger = logging.getLogger(__name__)


class StateManager(ABC):
    """Abstract interface for state persistence."""

    @abstractmethod
    async def save_state(self, state: Dict[str, Any]) -> None:
        """Save state to backend."""
        pass

    @abstractmethod
    async def load_state(self) -> Dict[str, Any]:
        """Load state from backend."""
        pass

    @abstractmethod
    async def checkpoint(self, state: Dict[str, Any]) -> None:
        """Create a checkpoint of the state."""
        pass

    @abstractmethod
    async def load_latest_checkpoint(self) -> Dict[str, Any]:
        """Load the most recent checkpoint."""
        pass


class InMemoryStateManager(StateManager):
    """
    In-memory state manager with optional JSON file persistence.

    Provides:
    - Fast in-memory state access
    - Optional periodic checkpoints to disk
    - Simple JSON serialization
    """

    def __init__(self, checkpoint_path: str = "./data/checkpoints/"):
        """
        Initialize in-memory state manager.

        Args:
            checkpoint_path: Directory for checkpoint files
        """
        self.state: Dict[str, Any] = {}
        self.checkpoint_path = Path(checkpoint_path)
        self.checkpoint_path.mkdir(parents=True, exist_ok=True)

    async def save_state(self, state: Dict[str, Any]) -> None:
        """Save state to memory."""
        self.state = state
        logger.debug("State saved to memory")

    async def load_state(self) -> Dict[str, Any]:
        """Load state from memory."""
        return self.state.copy()

    async def checkpoint(self, state: Dict[str, Any]) -> None:
        """
        Create a checkpoint file with timestamp.

        Format: checkpoint_YYYYMMDD_HHMMSS.json
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = self.checkpoint_path / f"checkpoint_{timestamp}.json"

        # Convert datetime objects to ISO format for JSON serialization
        serializable_state = self._make_serializable(state)

        try:
            async with aiofiles.open(checkpoint_file, 'w') as f:
                await f.write(json.dumps(serializable_state, indent=2))

            logger.info(f"Checkpoint created: {checkpoint_file}")

            # Cleanup old checkpoints (keep last 10)
            await self._cleanup_old_checkpoints(keep=10)

        except Exception as e:
            logger.error(f"Failed to create checkpoint: {e}")
            raise

    async def load_latest_checkpoint(self) -> Dict[str, Any]:
        """Load the most recent checkpoint file."""
        checkpoint_files = sorted(self.checkpoint_path.glob("checkpoint_*.json"))

        if not checkpoint_files:
            logger.info("No checkpoints found")
            return {}

        latest = checkpoint_files[-1]
        logger.info(f"Loading checkpoint: {latest}")

        try:
            async with aiofiles.open(latest, 'r') as f:
                content = await f.read()
                state = json.loads(content)

            # Convert ISO strings back to datetime objects
            state = self._deserialize_dates(state)

            return state

        except Exception as e:
            logger.error(f"Failed to load checkpoint {latest}: {e}")
            return {}

    def _make_serializable(self, obj: Any) -> Any:
        """Convert datetime objects to ISO strings for JSON serialization."""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj

    def _deserialize_dates(self, obj: Any) -> Any:
        """Convert ISO strings back to datetime objects."""
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                # Check if key suggests it's a timestamp field
                if isinstance(v, str) and ('timestamp' in k.lower() or 'update' in k.lower()):
                    try:
                        result[k] = datetime.fromisoformat(v)
                    except (ValueError, TypeError):
                        result[k] = v
                else:
                    result[k] = self._deserialize_dates(v)
            return result
        elif isinstance(obj, list):
            return [self._deserialize_dates(item) for item in obj]
        else:
            return obj

    async def _cleanup_old_checkpoints(self, keep: int = 10) -> None:
        """Remove old checkpoint files, keeping only the most recent."""
        checkpoint_files = sorted(self.checkpoint_path.glob("checkpoint_*.json"))

        if len(checkpoint_files) > keep:
            to_delete = checkpoint_files[:-keep]
            for f in to_delete:
                try:
                    os.remove(f)
                    logger.debug(f"Deleted old checkpoint: {f}")
                except Exception as e:
                    logger.warning(f"Failed to delete checkpoint {f}: {e}")


class FileStateManager(StateManager):
    """
    File-based state manager with single state file.

    Simpler than checkpointing - maintains single state.json file.
    """

    def __init__(self, state_file_path: str = "./data/state.json"):
        """
        Initialize file state manager.

        Args:
            state_file_path: Path to state file
        """
        self.state_file = Path(state_file_path)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    async def save_state(self, state: Dict[str, Any]) -> None:
        """Save state to file."""
        serializable_state = self._make_serializable(state)

        try:
            async with aiofiles.open(self.state_file, 'w') as f:
                await f.write(json.dumps(serializable_state, indent=2))
            logger.debug(f"State saved to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state: {e}")
            raise

    async def load_state(self) -> Dict[str, Any]:
        """Load state from file."""
        if not self.state_file.exists():
            logger.info("No state file found")
            return {}

        try:
            async with aiofiles.open(self.state_file, 'r') as f:
                content = await f.read()
                state = json.loads(content)
            return self._deserialize_dates(state)
        except Exception as e:
            logger.error(f"Failed to load state: {e}")
            return {}

    async def checkpoint(self, state: Dict[str, Any]) -> None:
        """Create checkpoint (same as save for file backend)."""
        await self.save_state(state)

    async def load_latest_checkpoint(self) -> Dict[str, Any]:
        """Load checkpoint (same as load for file backend)."""
        return await self.load_state()

    def _make_serializable(self, obj: Any) -> Any:
        """Convert datetime objects to ISO strings."""
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._make_serializable(item) for item in obj]
        elif isinstance(obj, datetime):
            return obj.isoformat()
        else:
            return obj

    def _deserialize_dates(self, obj: Any) -> Any:
        """Convert ISO strings back to datetime objects."""
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                if isinstance(v, str) and ('timestamp' in k.lower() or 'update' in k.lower()):
                    try:
                        result[k] = datetime.fromisoformat(v)
                    except (ValueError, TypeError):
                        result[k] = v
                else:
                    result[k] = self._deserialize_dates(v)
            return result
        elif isinstance(obj, list):
            return [self._deserialize_dates(item) for item in obj]
        else:
            return obj


def create_state_manager(backend: str, **kwargs) -> StateManager:
    """
    Factory function to create appropriate state manager.

    Args:
        backend: Backend type ('memory' or 'file')
        **kwargs: Backend-specific arguments

    Returns:
        StateManager instance
    """
    if backend == "memory":
        return InMemoryStateManager(
            checkpoint_path=kwargs.get('checkpoint_path', './data/checkpoints/')
        )
    elif backend == "file":
        return FileStateManager(
            state_file_path=kwargs.get('state_file_path', './data/state.json')
        )
    else:
        raise ValueError(f"Unknown state backend: {backend}")
