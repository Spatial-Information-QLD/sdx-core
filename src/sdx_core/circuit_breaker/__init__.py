"""Framework-agnostic async circuit breaker.

This package implements the circuit breaker pattern from *Release It!*.

Key behavior notes:
  - Storage persists only ``CLOSED`` and ``OPEN``. ``HALF_OPEN`` is an ephemeral,
    per-instance probe mode and is emitted to listeners for observability only.
  - Half-open probing is intentionally conservative: at most one in-flight probe
    call is permitted per ``CircuitBreaker`` instance.
  - If an excluded exception is raised during a probe, the probe is treated as
    if it never happened: no storage changes and no listener state changes. The
    circuit remains ``OPEN`` and a later call may attempt a fresh probe.
"""

from sdx_core.circuit_breaker.breaker import CircuitBreaker, CircuitBreakerConfig
from sdx_core.circuit_breaker.exceptions import (
    CircuitBreakerError,
    CircuitOpenError,
)
from sdx_core.circuit_breaker.metrics import BreakerListener
from sdx_core.circuit_breaker.state import BreakerSnapshot, CircuitState
from sdx_core.circuit_breaker.storage import (
    AbstractBreakerStorage,
    InMemoryBreakerStorage,
)

__all__ = [
    "AbstractBreakerStorage",
    "BreakerListener",
    "BreakerSnapshot",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerError",
    "CircuitOpenError",
    "CircuitState",
    "InMemoryBreakerStorage",
]
