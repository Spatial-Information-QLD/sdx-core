import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest

import sdx_core.circuit_breaker.breaker as breaker_mod
import sdx_core.circuit_breaker.storage as storage_mod
from sdx_core.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    InMemoryBreakerStorage,
)

pytestmark = pytest.mark.asyncio


class _FakeClock:
    def __init__(self, start: datetime | None = None) -> None:
        self._now = datetime(2020, 1, 1, tzinfo=UTC) if start is None else start

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now = self._now + timedelta(seconds=seconds)


@dataclass(slots=True)
class _RecordingListener:
    events: list[tuple[str, object]] = field(default_factory=list)

    async def on_state_change(self, name: str, old: CircuitState, new: CircuitState):
        self.events.append(("state", (name, old, new)))

    async def on_call_rejected(self, name: str):
        self.events.append(("rejected", name))

    async def on_call_succeeded(self, name: str, elapsed: float):
        self.events.append(("succeeded", name))

    async def on_call_failed(self, name: str, exc: Exception, elapsed: float):
        self.events.append(("failed", (name, exc.__class__.__name__)))


@dataclass(slots=True)
class _ExplodingListener:
    async def on_state_change(self, name: str, old: CircuitState, new: CircuitState):
        raise RuntimeError("boom")

    async def on_call_rejected(self, name: str):
        raise RuntimeError("boom")

    async def on_call_succeeded(self, name: str, elapsed: float):
        raise RuntimeError("boom")

    async def on_call_failed(self, name: str, exc: Exception, elapsed: float):
        raise RuntimeError("boom")


async def test_closed_call_succeeds_and_stays_closed() -> None:
    storage = InMemoryBreakerStorage()
    breaker = CircuitBreaker(
        "svc",
        config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout=1.0),
        storage=storage,
    )

    async def _ok() -> str:
        return "ok"

    assert await breaker.call(_ok) == "ok"
    snapshot = await storage.get_state("svc")
    assert snapshot.state == CircuitState.CLOSED
    assert snapshot.failure_count == 0


async def test_call_with_async_callable_instance_succeeds_and_stays_closed() -> None:
    storage = InMemoryBreakerStorage()
    breaker = CircuitBreaker(
        "svc",
        config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout=1.0),
        storage=storage,
    )

    class _AsyncCallable:
        async def __call__(self) -> str:
            return "ok"

    assert await breaker.call(_AsyncCallable()) == "ok"
    snapshot = await storage.get_state("svc")
    assert snapshot.state == CircuitState.CLOSED
    assert snapshot.failure_count == 0


async def test_failure_threshold_opens_and_rejects_calls() -> None:
    storage = InMemoryBreakerStorage()
    breaker = CircuitBreaker(
        "svc",
        config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout=10.0),
        storage=storage,
    )

    async def _fail() -> None:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        await breaker.call(_fail)

    with pytest.raises(CircuitOpenError) as excinfo:
        await breaker.call(_fail)

    assert 0.0 < excinfo.value.retry_after <= 10.0
    snapshot = await storage.get_state("svc")
    assert snapshot.state == CircuitState.OPEN


async def test_half_open_allows_single_probe_and_rejects_concurrent() -> None:
    storage = InMemoryBreakerStorage()
    breaker = CircuitBreaker(
        "svc",
        config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.0),
        storage=storage,
    )

    async def _fail() -> None:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        await breaker.call(_fail)

    started = asyncio.Event()
    release = asyncio.Event()

    async def _probe() -> str:
        started.set()
        await release.wait()
        return "ok"

    task = asyncio.create_task(breaker.call(_probe))
    await started.wait()

    async def _ok() -> str:
        return "ok"

    with pytest.raises(CircuitOpenError) as excinfo:
        await breaker.call(_ok)
    assert excinfo.value.retry_after == 0.0

    release.set()
    assert await task == "ok"

    assert await breaker.call(_ok) == "ok"
    snapshot = await storage.get_state("svc")
    assert snapshot.state == CircuitState.CLOSED


async def test_probe_failure_reopens_and_restarts_timeout(monkeypatch) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(breaker_mod, "_utcnow", clock.now)
    monkeypatch.setattr(storage_mod, "_utcnow", clock.now)

    storage = InMemoryBreakerStorage()
    breaker = CircuitBreaker(
        "svc",
        config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout=5.0),
        storage=storage,
    )

    async def _fail() -> None:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        await breaker.call(_fail)

    clock.advance(5.0)
    with pytest.raises(RuntimeError):
        await breaker.call(_fail)

    with pytest.raises(CircuitOpenError) as excinfo:
        await breaker.call(_fail)
    assert excinfo.value.retry_after > 0.0


async def test_excluded_exception_during_probe_is_neutral(monkeypatch) -> None:
    class _Excluded(Exception):
        pass

    clock = _FakeClock()
    monkeypatch.setattr(breaker_mod, "_utcnow", clock.now)
    monkeypatch.setattr(storage_mod, "_utcnow", clock.now)

    storage = InMemoryBreakerStorage()
    listener = _RecordingListener()
    breaker = CircuitBreaker(
        "svc",
        config=CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=5.0,
            expected_exceptions=(Exception,),
            excluded_exceptions=(_Excluded,),
        ),
        storage=storage,
        listeners=[listener],
    )

    async def _fail() -> None:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        await breaker.call(_fail)

    listener.events.clear()
    clock.advance(5.0)

    async def _excluded_probe() -> None:
        raise _Excluded("ignored")

    with pytest.raises(_Excluded):
        await breaker.call(_excluded_probe)

    snapshot = await storage.get_state("svc")
    assert snapshot.state == CircuitState.OPEN
    assert listener.events == []

    ran = False

    async def _probe_ok() -> str:
        nonlocal ran
        ran = True
        return "ok"

    assert await breaker.call(_probe_ok) == "ok"
    assert ran is True
    snapshot = await storage.get_state("svc")
    assert snapshot.state == CircuitState.CLOSED


async def test_listener_exceptions_are_swallowed() -> None:
    storage = InMemoryBreakerStorage()
    recording = _RecordingListener()
    breaker = CircuitBreaker(
        "svc",
        config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.0),
        storage=storage,
        listeners=[_ExplodingListener(), recording],
    )

    async def _ok() -> str:
        return "ok"

    assert await breaker.call(_ok) == "ok"
    assert ("succeeded", "svc") in recording.events


async def test_config_rejects_invalid_threshold_and_timeout() -> None:
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreakerConfig(failure_threshold=0)
    with pytest.raises(ValueError, match="recovery_timeout"):
        CircuitBreakerConfig(recovery_timeout=-1.0)


async def test_probe_gate_uses_thread_lock_when_gil_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sdx_core.circuit_breaker.breaker.sys._is_gil_enabled",
        lambda: False,
        raising=False,
    )
    gate = breaker_mod._ProbeGate()

    assert gate.try_acquire() is True
    assert gate.try_acquire() is False
    gate.release()
    assert gate.try_acquire() is True
    gate.release()


async def test_listener_exceptions_are_swallowed_for_all_non_success_events() -> None:
    storage = InMemoryBreakerStorage()
    recording = _RecordingListener()
    breaker = CircuitBreaker(
        "svc",
        config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout=10.0),
        storage=storage,
        listeners=[_ExplodingListener(), recording],
    )

    async def _fail() -> None:
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        await breaker.call(_fail)

    with pytest.raises(CircuitOpenError):
        await breaker.call(_fail)

    assert ("failed", ("svc", "RuntimeError")) in recording.events
    assert (
        "state",
        ("svc", CircuitState.CLOSED, CircuitState.OPEN),
    ) in recording.events
    assert ("rejected", "svc") in recording.events
