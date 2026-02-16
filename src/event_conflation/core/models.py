"""Core data models for event conflation system."""

from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, field_validator


class Event(BaseModel):
    """Input event model."""

    id: str = Field(..., description="Unique object identifier")
    timestamp: datetime = Field(..., description="Event timestamp (ISO8601 or Unix epoch)")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary event data")

    @field_validator('timestamp', mode='before')
    @classmethod
    def parse_timestamp(cls, v):
        """Parse timestamp from ISO8601 string or Unix epoch."""
        if isinstance(v, datetime):
            return v
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v)
        if isinstance(v, str):
            # Try ISO8601 format
            return datetime.fromisoformat(v.replace('Z', '+00:00'))
        raise ValueError(f"Invalid timestamp format: {v}")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ConflatedRecord(BaseModel):
    """Output conflated record (DynamoDB format)."""

    id: str = Field(..., description="Unique object identifier")
    first_event_timestamp: datetime = Field(..., description="Timestamp of first event")
    last_event_timestamp: datetime = Field(..., description="Timestamp of last event")
    event_count: int = Field(..., gt=0, description="Number of events conflated")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ConflationState(BaseModel):
    """Internal state for an event processor."""

    id: str
    first_timestamp: Optional[datetime] = None
    last_timestamp: Optional[datetime] = None
    event_count: int = 0
    last_update: Optional[datetime] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ConflationConfig(BaseModel):
    """Configuration for conflation behavior."""

    # Conflation settings
    idle_interval: float = Field(default=30.0, description="Seconds of idle time before emitting")
    max_buffer_size: int = Field(default=10000, description="Max concurrent event IDs")

    # State settings
    state_backend: str = Field(default="memory", description="State backend: memory, file")
    checkpoint_interval: int = Field(default=60, description="Checkpoint interval in seconds")
    state_file_path: str = Field(default="./data/state.json", description="Path to state file")
    checkpoint_path: str = Field(default="./data/checkpoints/", description="Path to checkpoint dir")

    # Output settings
    output_writer: str = Field(default="json", description="Output writer type")
    output_file_path: str = Field(default="./data/conflated_events.json", description="Output file path")
    enable_batching: bool = Field(default=True, description="Enable output batching")
    batch_size: int = Field(default=100, description="Batch size for output")

    # Error handling
    max_clock_skew: int = Field(default=300, description="Max allowed clock skew in seconds")
    dlq_path: str = Field(default="./data/dlq.json", description="Dead letter queue path")

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format: json or text")

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "ConflationConfig":
        """Create config from nested dictionary structure."""
        flat_config = {}

        # Flatten nested config structure
        if 'conflation' in config_dict:
            flat_config['idle_interval'] = config_dict['conflation'].get('idle_interval', 30.0)
            flat_config['max_buffer_size'] = config_dict['conflation'].get('max_buffer_size', 10000)

        if 'state' in config_dict:
            flat_config['state_backend'] = config_dict['state'].get('backend', 'memory')
            flat_config['checkpoint_interval'] = config_dict['state'].get('checkpoint_interval', 60)
            if 'file' in config_dict['state']:
                flat_config['state_file_path'] = config_dict['state']['file'].get('state_path', './data/state.json')
                flat_config['checkpoint_path'] = config_dict['state']['file'].get('checkpoint_path', './data/checkpoints/')

        if 'output' in config_dict:
            flat_config['output_writer'] = config_dict['output'].get('writer', 'json')
            flat_config['output_file_path'] = config_dict['output'].get('file_path', './data/conflated_events.json')
            flat_config['enable_batching'] = config_dict['output'].get('enable_batching', True)
            flat_config['batch_size'] = config_dict['output'].get('batch_size', 100)

        if 'error_handling' in config_dict:
            flat_config['max_clock_skew'] = config_dict['error_handling'].get('max_clock_skew', 300)
            flat_config['dlq_path'] = config_dict['error_handling'].get('dlq_path', './data/dlq.json')

        if 'logging' in config_dict:
            flat_config['log_level'] = config_dict['logging'].get('level', 'INFO')
            flat_config['log_format'] = config_dict['logging'].get('format', 'json')

        return cls(**flat_config)
