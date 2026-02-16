"""Custom exceptions for event conflation system."""


class ConflationError(Exception):
    """Base exception for conflation errors."""
    pass


class ValidationError(ConflationError):
    """Event validation failed."""
    pass


class StateError(ConflationError):
    """State management error."""
    pass


class BufferOverflowError(ConflationError):
    """Max buffer size exceeded."""
    pass


class ConfigurationError(ConflationError):
    """Configuration is invalid."""
    pass


class ClockSkewError(ConflationError):
    """Clock skew detected beyond acceptable threshold."""
    pass
