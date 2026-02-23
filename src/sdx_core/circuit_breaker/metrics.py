"""Observability hooks for circuit breakers."""

from typing import Protocol

from sdx_core.circuit_breaker.state import CircuitState


class BreakerListener(Protocol):
    """Listener protocol for circuit breaker events.

    Notes:
        ``on_state_change(OPEN → HALF_OPEN)`` is emitted per probe attempt per
        breaker instance. Storage does not persist ``HALF_OPEN``.
    """

    async def on_state_change(
        self, name: str, old: CircuitState, new: CircuitState
    ) -> None:
        """Handle circuit state transitions."""

    async def on_call_rejected(self, name: str) -> None:
        """Handle call rejection while the circuit is open."""

    async def on_call_succeeded(self, name: str, elapsed: float) -> None:
        """Handle successful protected call completion."""

    async def on_call_failed(self, name: str, exc: Exception, elapsed: float) -> None:
        """Handle failed protected call completion."""
