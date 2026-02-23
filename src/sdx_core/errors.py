"""Shared error types for sdx_core."""


class TransientError(RuntimeError):
    """Generic retry-safe transient dependency failure."""
