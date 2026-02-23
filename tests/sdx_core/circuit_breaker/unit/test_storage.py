from __future__ import annotations

import pytest

from sdx_core.circuit_breaker import CircuitState, InMemoryBreakerStorage

pytestmark = pytest.mark.asyncio


class _ExplodingAsyncLock:
    async def acquire(self) -> None:
        raise RuntimeError("async acquire failed")

    def release(self) -> None:
        return


async def test_record_success_resets_non_healthy_snapshot() -> None:
    storage = InMemoryBreakerStorage()
    await storage.record_failure("svc")

    snapshot = await storage.record_success("svc")

    assert snapshot.state == CircuitState.CLOSED
    assert snapshot.failure_count == 0
    assert snapshot.last_failure_at is None
    assert snapshot.opened_at is None


async def test_storage_uses_thread_lock_path_when_gil_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sdx_core.circuit_breaker.storage.sys._is_gil_enabled",
        lambda: False,
        raising=False,
    )
    storage = InMemoryBreakerStorage()

    snapshot = await storage.get_state("svc")

    assert snapshot.name == "svc"


async def test_storage_releases_thread_lock_if_async_lock_acquire_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "sdx_core.circuit_breaker.storage.sys._is_gil_enabled",
        lambda: False,
        raising=False,
    )
    storage = InMemoryBreakerStorage()

    thread_lock = storage._thread_locks["svc"]
    storage._async_locks["svc"] = _ExplodingAsyncLock()  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match="async acquire failed"):
        async with storage._locked("svc"):
            pass

    assert thread_lock.locked() is False
