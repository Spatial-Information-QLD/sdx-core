"""Circuit breaker state primitives."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class CircuitState(StrEnum):
    """Circuit breaker state values."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class BreakerSnapshot:
    """Point-in-time view of breaker internals useful for metrics/logging.

    Attributes:
        name: Breaker name.
        state: Persisted breaker state.
        failure_count: Count of recent failures while ``CLOSED``.
        last_failure_at: Timestamp of the last counted failure, if any.
        opened_at: Timestamp when the breaker entered ``OPEN``, if open.
    """

    name: str
    state: CircuitState
    failure_count: int
    last_failure_at: datetime | None
    opened_at: datetime | None
