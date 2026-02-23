"""State storage for circuit breakers.

Storage is intentionally decoupled from breaker logic. Custom backends (for
example Redis) can implement the interface for multi-process coordination.

Important: storage persists only ``CLOSED`` and ``OPEN``. ``HALF_OPEN`` is an
ephemeral, per-instance probe mode and should not be persisted by backends.
"""

import asyncio
import sys
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from sdx_core.circuit_breaker.state import BreakerSnapshot, CircuitState


def _utcnow() -> datetime:
    return datetime.now(UTC)


class AbstractBreakerStorage(ABC):
    """Abstract breaker storage interface."""

    @abstractmethod
    async def get_state(self, name: str) -> BreakerSnapshot:
        """Return the current breaker snapshot for ``name``."""

    @abstractmethod
    async def record_success(self, name: str) -> BreakerSnapshot:
        """Record a successful call and return the updated snapshot."""

    @abstractmethod
    async def record_failure(self, name: str) -> BreakerSnapshot:
        """Record a failed call and return the updated snapshot."""

    @abstractmethod
    async def force_open(self, name: str) -> BreakerSnapshot:
        """Force breaker ``name`` into ``OPEN`` state."""

    @abstractmethod
    async def reset(self, name: str) -> BreakerSnapshot:
        """Reset breaker ``name`` to a healthy ``CLOSED`` state."""


class InMemoryBreakerStorage(AbstractBreakerStorage):
    """In-memory storage with per-breaker cooperative + optional thread locks."""

    def __init__(self) -> None:
        """Initialize in-memory snapshot and lock registries."""
        self._snapshots: dict[str, BreakerSnapshot] = {}
        self._async_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._thread_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
        is_gil_enabled = getattr(sys, "_is_gil_enabled", None)
        self._gil_enabled = True if is_gil_enabled is None else bool(is_gil_enabled())

    def _default_snapshot(self, name: str) -> BreakerSnapshot:
        return BreakerSnapshot(
            name=name,
            state=CircuitState.CLOSED,
            failure_count=0,
            last_failure_at=None,
            opened_at=None,
        )

    @asynccontextmanager
    async def _locked(self, name: str) -> AsyncIterator[None]:
        async_lock = self._async_locks[name]
        if self._gil_enabled:
            await async_lock.acquire()
            try:
                yield
            finally:
                async_lock.release()
            return

        thread_lock = self._thread_locks[name]
        thread_lock.acquire()
        try:
            await async_lock.acquire()
        except Exception:
            thread_lock.release()
            raise
        try:
            yield
        finally:
            async_lock.release()
            thread_lock.release()

    async def get_state(self, name: str) -> BreakerSnapshot:
        """Return the current snapshot, creating a default one if missing."""
        async with self._locked(name):
            snapshot = self._snapshots.get(name)
            if snapshot is None:
                snapshot = self._default_snapshot(name)
                self._snapshots[name] = snapshot
            return snapshot

    async def record_success(self, name: str) -> BreakerSnapshot:
        """Record a successful call.

        Storage backends may treat this as a no-op when already healthy (for
        example, failure_count is already 0) to avoid hot-path writes.
        """
        async with self._locked(name):
            snapshot = self._snapshots.get(name, self._default_snapshot(name))
            if (
                snapshot.state == CircuitState.CLOSED
                and snapshot.failure_count == 0
                and snapshot.last_failure_at is None
                and snapshot.opened_at is None
            ):
                self._snapshots[name] = snapshot
                return snapshot
            updated = BreakerSnapshot(
                name=name,
                state=CircuitState.CLOSED,
                failure_count=0,
                last_failure_at=None,
                opened_at=None,
            )
            self._snapshots[name] = updated
            return updated

    async def record_failure(self, name: str) -> BreakerSnapshot:
        """Increment failure counters and return the updated snapshot."""
        async with self._locked(name):
            snapshot = self._snapshots.get(name, self._default_snapshot(name))
            updated = BreakerSnapshot(
                name=name,
                state=snapshot.state,
                failure_count=snapshot.failure_count + 1,
                last_failure_at=_utcnow(),
                opened_at=snapshot.opened_at,
            )
            self._snapshots[name] = updated
            return updated

    async def force_open(self, name: str) -> BreakerSnapshot:
        """Force the circuit open and restart the recovery timeout window."""
        async with self._locked(name):
            snapshot = self._snapshots.get(name, self._default_snapshot(name))
            updated = BreakerSnapshot(
                name=name,
                state=CircuitState.OPEN,
                failure_count=snapshot.failure_count,
                last_failure_at=snapshot.last_failure_at,
                opened_at=_utcnow(),
            )
            self._snapshots[name] = updated
            return updated

    async def reset(self, name: str) -> BreakerSnapshot:
        """Reset breaker state and counters to a healthy default snapshot."""
        async with self._locked(name):
            updated = BreakerSnapshot(
                name=name,
                state=CircuitState.CLOSED,
                failure_count=0,
                last_failure_at=None,
                opened_at=None,
            )
            self._snapshots[name] = updated
            return updated
