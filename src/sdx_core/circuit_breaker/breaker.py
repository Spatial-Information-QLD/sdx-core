"""Core circuit breaker implementation."""

import asyncio
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import ParamSpec, TypeVar

from sdx_core.circuit_breaker.exceptions import CircuitOpenError
from sdx_core.circuit_breaker.metrics import BreakerListener
from sdx_core.circuit_breaker.state import BreakerSnapshot, CircuitState
from sdx_core.circuit_breaker.storage import (
    AbstractBreakerStorage,
    InMemoryBreakerStorage,
)

T = TypeVar("T")
P = ParamSpec("P")


def _utcnow() -> datetime:
    return datetime.now(UTC)


class _ProbeGate:
    """Allow at most one in-flight half-open probe per breaker instance."""

    def __init__(self) -> None:
        is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
        self._gil_enabled = True if is_gil_enabled is None else bool(is_gil_enabled())
        self._thread_lock: threading.Lock | None = None
        if not self._gil_enabled:
            self._thread_lock = threading.Lock()
        self._held = False

    def try_acquire(self) -> bool:
        if self._thread_lock is None:
            if self._held:
                return False
            self._held = True
            return True

        with self._thread_lock:
            if self._held:
                return False
            self._held = True
            return True

    def release(self) -> None:
        if self._thread_lock is None:
            self._held = False
            return
        with self._thread_lock:
            self._held = False


@dataclass(slots=True)
class CircuitBreakerConfig:
    """Circuit breaker configuration values.

    Attributes:
        failure_threshold: Failures required while ``CLOSED`` before opening.
        recovery_timeout: Seconds to wait while ``OPEN`` before allowing a probe.
        expected_exceptions: Exceptions that count as failures.
        excluded_exceptions: Exceptions that must not count as failures.
    """

    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    expected_exceptions: tuple[type[Exception], ...] = (Exception,)
    excluded_exceptions: tuple[type[Exception], ...] = ()

    def __post_init__(self) -> None:
        if self.failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if self.recovery_timeout < 0:
            raise ValueError("recovery_timeout must be >= 0")


class CircuitBreaker:
    """Stateful proxy around a dangerous async operation."""

    def __init__(
        self,
        name: str,
        *,
        config: CircuitBreakerConfig | None = None,
        storage: AbstractBreakerStorage | None = None,
        listeners: Sequence[BreakerListener] | None = None,
    ) -> None:
        """Build a circuit breaker with optional custom dependencies.

        Args:
            name: Unique breaker name used for storage and metrics.
            config: Breaker behavior configuration. Defaults to
                ``CircuitBreakerConfig()``.
            storage: State storage backend. Defaults to in-memory storage.
            listeners: Optional listener hooks for breaker events.
        """
        self.name = name
        self.config = CircuitBreakerConfig() if config is None else config
        self._storage = InMemoryBreakerStorage() if storage is None else storage
        self._listeners = tuple(listeners) if listeners is not None else ()
        self._probe_gate = _ProbeGate()

    async def _emit_state_change(self, old: CircuitState, new: CircuitState) -> None:
        for listener in self._listeners:
            try:
                await listener.on_state_change(self.name, old, new)
            except Exception:
                continue

    async def _emit_call_rejected(self) -> None:
        for listener in self._listeners:
            try:
                await listener.on_call_rejected(self.name)
            except Exception:
                continue

    async def _emit_call_succeeded(self, elapsed: float) -> None:
        for listener in self._listeners:
            try:
                await listener.on_call_succeeded(self.name, elapsed)
            except Exception:
                continue

    async def _emit_call_failed(self, exc: Exception, elapsed: float) -> None:
        for listener in self._listeners:
            try:
                await listener.on_call_failed(self.name, exc, elapsed)
            except Exception:
                continue

    @staticmethod
    def _retry_after(snapshot: BreakerSnapshot, now: datetime, timeout: float) -> float:
        opened_at = now if snapshot.opened_at is None else snapshot.opened_at
        elapsed = (now - opened_at).total_seconds()
        return max(timeout - elapsed, 0.0)

    async def call(
        self,
        func: Callable[P, Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Invoke an async callable under circuit breaker protection.

        Args:
            func: Dangerous async callable to execute.
            *args: Positional arguments forwarded to ``func``.
            **kwargs: Keyword arguments forwarded to ``func``.

        Returns:
            The result of ``func`` when allowed and successful.

        Raises:
            CircuitOpenError: When the circuit is open and the call is rejected.
            Exception: The original exception from ``func`` when it is attempted
                and fails with an expected exception type.
        """
        task = asyncio.current_task()
        if task is not None:
            callable_name = getattr(func, "__qualname__", None)
            if callable_name is None:
                callable_name = getattr(func, "__name__", None)
            if callable_name is None:
                callable_name = func.__class__.__qualname__
            task.set_name(f"circuit_breaker:{self.name}:{str(callable_name)}")

        snapshot = await self._storage.get_state(self.name)
        now = _utcnow()
        is_probe = False
        probe_acquired = False

        if snapshot.state == CircuitState.OPEN:
            retry_after = self._retry_after(snapshot, now, self.config.recovery_timeout)
            if retry_after > 0:
                await self._emit_call_rejected()
                raise CircuitOpenError(self.name, retry_after=retry_after)

            if not self._probe_gate.try_acquire():
                await self._emit_call_rejected()
                raise CircuitOpenError(self.name, retry_after=0.0)

            is_probe = True
            probe_acquired = True

        start = time.monotonic()
        try:
            result = await func(*args, **kwargs)
        except self.config.excluded_exceptions:
            raise
        except self.config.expected_exceptions as exc:
            elapsed = max(time.monotonic() - start, 0.0)
            await self._emit_call_failed(exc, elapsed)

            if is_probe:
                await self._emit_state_change(CircuitState.OPEN, CircuitState.HALF_OPEN)
                await self._storage.record_failure(self.name)
                await self._storage.force_open(self.name)
                await self._emit_state_change(CircuitState.HALF_OPEN, CircuitState.OPEN)
            else:
                failure_snapshot = await self._storage.record_failure(self.name)
                if failure_snapshot.failure_count >= self.config.failure_threshold:
                    await self._storage.force_open(self.name)
                    await self._emit_state_change(
                        CircuitState.CLOSED, CircuitState.OPEN
                    )
            raise
        else:
            elapsed = max(time.monotonic() - start, 0.0)

            if is_probe:
                await self._emit_state_change(CircuitState.OPEN, CircuitState.HALF_OPEN)
                await self._storage.reset(self.name)
                await self._emit_state_change(
                    CircuitState.HALF_OPEN, CircuitState.CLOSED
                )
            else:
                await self._storage.record_success(self.name)

            await self._emit_call_succeeded(elapsed)
            return result
        finally:
            if probe_acquired:
                self._probe_gate.release()
