from __future__ import annotations

import asyncio
import threading
import time
from typing import cast

import pytest
from confluent_kafka.admin import AdminClient

import sdx_core.readiness as readiness_mod
from sdx_core.readiness import (
    REASON_CHECK_FAILED,
    REASON_READY,
    CheckResult,
    ReadinessManager,
    ReadinessSnapshot,
    build_starting_snapshot,
    evaluate_readiness_once,
    make_callable_check,
    make_kafka_topics_check,
)

pytestmark = pytest.mark.asyncio


class _FakeTopicMetadata:
    def __init__(self, error: object | None = None) -> None:
        self.error = error


class _FakeClusterMetadata:
    def __init__(self, topics: dict[str, _FakeTopicMetadata]) -> None:
        self.topics = topics


class _FakeAdminClient:
    scripted_results: list[object] = []
    instances: list[_FakeAdminClient] = []

    def __init__(self, _config: dict[str, object]) -> None:
        self._results = list(self.scripted_results)
        self.list_topics_calls = 0
        _FakeAdminClient.instances.append(self)

    def list_topics(self, timeout: float) -> object:
        self.list_topics_calls += 1
        self.last_timeout = timeout
        if not self._results:
            return _FakeClusterMetadata({})
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _metadata_for(
    topics: set[str],
    *,
    errored_topics: set[str] | None = None,
) -> _FakeClusterMetadata:
    values: dict[str, _FakeTopicMetadata] = {}
    for topic in topics:
        if errored_topics is not None and topic in errored_topics:
            values[topic] = _FakeTopicMetadata(error=RuntimeError("errored"))
        else:
            values[topic] = _FakeTopicMetadata()
    return _FakeClusterMetadata(values)


def _reset_admin_fakes() -> None:
    _FakeAdminClient.scripted_results = []
    _FakeAdminClient.instances.clear()


async def test_build_starting_snapshot_marks_all_checks_not_ready() -> None:
    snapshot = build_starting_snapshot(("kafka", "esri"), now_fn=lambda: 123.0)

    assert snapshot.ready is False
    assert snapshot.status == "degraded"
    assert snapshot.reason == "starting"
    assert snapshot.last_checked_at == 123.0
    assert len(snapshot.check_results) == 2
    assert all(result.ok is False for result in snapshot.check_results)


async def test_check_result_data_is_read_only_mapping() -> None:
    result = CheckResult(name="check", ok=True, data={"k": "v"})

    with pytest.raises(TypeError):
        cast(dict[str, object], result.data)["x"] = "y"


async def test_make_kafka_topics_check_returns_read_only_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_admin_fakes()
    _FakeAdminClient.scripted_results = [_metadata_for({"in", "out"})]
    monkeypatch.setattr(readiness_mod, "AdminClient", _FakeAdminClient)

    check = make_kafka_topics_check(
        admin_config={"bootstrap.servers": "localhost:9092"},
        required_topics=("in", "out"),
        timeout_seconds=2.0,
    )
    result = await check()

    with pytest.raises(TypeError):
        cast(dict[str, object], result.data)["new"] = "value"


async def test_evaluate_readiness_once_all_checks_ok() -> None:
    previous = build_starting_snapshot(("kafka", "esri"), now_fn=lambda: 1.0)

    async def _kafka() -> CheckResult:
        return CheckResult(name="kafka", ok=True)

    async def _esri() -> CheckResult:
        return CheckResult(name="esri", ok=True)

    snapshot = await evaluate_readiness_once(
        checks=(_kafka, _esri),
        previous=previous,
        source="startup",
        now_fn=lambda: 2.0,
    )

    assert snapshot.ready is True
    assert snapshot.status == "ok"
    assert snapshot.reason == REASON_READY
    assert snapshot.last_checked_at == 2.0


async def test_evaluate_readiness_once_uses_first_failed_reason() -> None:
    previous = build_starting_snapshot(("kafka", "esri"), now_fn=lambda: 1.0)

    async def _kafka() -> CheckResult:
        return CheckResult(
            name="kafka",
            ok=False,
            reason="kafka_unreachable",
            detail="broker down",
        )

    async def _esri() -> CheckResult:
        return CheckResult(name="esri", ok=False, reason="dependency_unavailable")

    snapshot = await evaluate_readiness_once(
        checks=(_kafka, _esri),
        previous=previous,
        source="background",
    )

    assert snapshot.ready is False
    assert snapshot.reason == "kafka_unreachable"
    assert snapshot.detail == "broker down"


async def test_evaluate_readiness_once_marks_exceptions_as_failed_checks() -> None:
    previous = build_starting_snapshot(("kafka",), now_fn=lambda: 1.0)

    async def _kafka() -> CheckResult:
        raise RuntimeError("boom")

    snapshot = await evaluate_readiness_once(
        checks=(_kafka,),
        previous=previous,
        source="background",
    )

    assert snapshot.ready is False
    assert snapshot.reason == REASON_CHECK_FAILED
    failed_check = snapshot.check_results[0]
    assert failed_check.name == "_kafka"
    assert failed_check.ok is False
    assert failed_check.reason == REASON_CHECK_FAILED
    assert "RuntimeError: boom" in failed_check.detail


async def test_evaluate_readiness_once_suppresses_on_snapshot_exception_and_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    previous = build_starting_snapshot(("kafka",), now_fn=lambda: 1.0)

    async def _kafka() -> CheckResult:
        return CheckResult(name="kafka", ok=True)

    def _raise_callback(
        previous: ReadinessSnapshot | None,
        current: ReadinessSnapshot,
        *,
        source: str,
    ) -> None:
        _ = (previous, current, source)
        raise RuntimeError("callback boom")

    with caplog.at_level("WARNING", logger="sdx_core.readiness"):
        snapshot = await evaluate_readiness_once(
            checks=(_kafka,),
            previous=previous,
            source="startup",
            on_snapshot=_raise_callback,
        )

    assert snapshot.ready is True
    assert snapshot.reason == REASON_READY
    callback_logs = [
        record
        for record in caplog.records
        if record.message == "Readiness snapshot callback failed; continuing"
    ]
    assert callback_logs
    assert callback_logs[0].exc_info is not None
    assert getattr(callback_logs[0], "source", None) == "startup"
    assert getattr(callback_logs[0], "ready", None) is True
    assert getattr(callback_logs[0], "reason", None) == REASON_READY
    assert getattr(callback_logs[0], "callback", None) == "_raise_callback"


async def test_readiness_manager_background_survives_on_snapshot_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def _always_ready() -> bool:
        return True

    def _raise_callback(
        previous: ReadinessSnapshot | None,
        current: ReadinessSnapshot,
        *,
        source: str,
    ) -> None:
        _ = (previous, current, source)
        raise RuntimeError("callback boom")

    manager = ReadinessManager(
        checks=(
            make_callable_check(
                name="dep",
                check=_always_ready,
                reason_when_false="dependency_unavailable",
                detail_when_false="not ready",
            ),
        ),
        interval_seconds=0.01,
        on_snapshot=_raise_callback,
    )

    with caplog.at_level("WARNING", logger="sdx_core.readiness"):
        await manager.start_background()
        deadline = time.monotonic() + 1.0
        while not manager.snapshot.ready and time.monotonic() < deadline:
            await asyncio.sleep(0.01)
        await manager.stop_background()

    assert manager.snapshot.ready is True
    callback_logs = [
        record
        for record in caplog.records
        if record.message == "Readiness snapshot callback failed; continuing"
    ]
    assert callback_logs


@pytest.mark.parametrize(
    ("result_value", "expected_ok", "expected_reason"),
    [
        (True, True, None),
        (False, False, "dependency_unavailable"),
    ],
)
async def test_make_callable_check_with_sync_callable(
    result_value: bool,
    expected_ok: bool,
    expected_reason: str | None,
) -> None:
    def _check_value() -> bool:
        return result_value

    check = make_callable_check(
        name="esri",
        check=_check_value,
        reason_when_false="dependency_unavailable",
        detail_when_false="Feature service unavailable.",
    )

    result = await check()
    assert result.ok is expected_ok
    assert result.reason == expected_reason


async def test_make_callable_check_with_async_callable() -> None:
    async def _check_value() -> bool:
        return True

    check = make_callable_check(
        name="esri",
        check=_check_value,
        reason_when_false="dependency_unavailable",
        detail_when_false="Feature service unavailable.",
    )

    result = await check()
    assert result.ok is True


async def test_make_callable_check_exception_maps_to_check_failed() -> None:
    def _check_value() -> bool:
        raise RuntimeError("boom")

    check = make_callable_check(
        name="esri",
        check=_check_value,
        reason_when_false="dependency_unavailable",
        detail_when_false="Feature service unavailable.",
    )

    result = await check()
    assert result.ok is False
    assert result.reason == REASON_CHECK_FAILED
    assert "RuntimeError: boom" in result.detail


async def test_make_kafka_topics_check_broker_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_admin_fakes()
    _FakeAdminClient.scripted_results = [RuntimeError("broker down")]
    monkeypatch.setattr(readiness_mod, "AdminClient", _FakeAdminClient)

    check = make_kafka_topics_check(
        admin_config={"bootstrap.servers": "localhost:9092"},
        required_topics=("in", "out"),
        timeout_seconds=2.0,
    )
    result = await check()
    assert result.ok is False
    assert result.reason == "kafka_unreachable"
    assert result.data["kafka_broker_ok"] is False


async def test_make_kafka_topics_check_missing_topics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_admin_fakes()
    _FakeAdminClient.scripted_results = [_metadata_for({"in"})]
    monkeypatch.setattr(readiness_mod, "AdminClient", _FakeAdminClient)

    check = make_kafka_topics_check(
        admin_config={"bootstrap.servers": "localhost:9092"},
        required_topics=("in", "out"),
        timeout_seconds=2.0,
    )
    result = await check()
    assert result.ok is False
    assert result.reason == "topic_missing"
    missing_topics = cast(tuple[str, ...], result.data["missing_topics"])
    assert "out" in missing_topics


async def test_make_kafka_topics_check_topic_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_admin_fakes()
    _FakeAdminClient.scripted_results = [
        _metadata_for({"in", "out"}, errored_topics={"out"})
    ]
    monkeypatch.setattr(readiness_mod, "AdminClient", _FakeAdminClient)

    check = make_kafka_topics_check(
        admin_config={"bootstrap.servers": "localhost:9092"},
        required_topics=("in", "out"),
        timeout_seconds=2.0,
    )
    result = await check()
    assert result.ok is False
    assert result.reason == "topic_missing"
    errored_topics = cast(tuple[str, ...], result.data["errored_topics"])
    assert "out" in errored_topics


async def test_make_kafka_topics_check_all_ok(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_admin_fakes()
    _FakeAdminClient.scripted_results = [_metadata_for({"in", "out"})]
    monkeypatch.setattr(readiness_mod, "AdminClient", _FakeAdminClient)

    check = make_kafka_topics_check(
        admin_config={"bootstrap.servers": "localhost:9092"},
        required_topics=("in", "out"),
        timeout_seconds=2.0,
    )
    result = await check()
    assert result.ok is True
    assert result.data["kafka_topics_ok"] is True


async def test_make_kafka_topics_check_timeout_returns_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _SlowAdminClient:
        def __init__(self, _config: dict[str, object]) -> None:
            self._config = _config

        def list_topics(self, timeout: float) -> object:
            time.sleep(0.05)
            return _metadata_for({"in", "out"})

    monkeypatch.setattr(readiness_mod, "AdminClient", _SlowAdminClient)
    check = make_kafka_topics_check(
        admin_config={"bootstrap.servers": "localhost:9092"},
        required_topics=("in", "out"),
        timeout_seconds=0.001,
    )
    result = await check()
    assert result.ok is False
    assert result.reason == "kafka_unreachable"
    assert result.data["kafka_broker_ok"] is False
    assert result.data["kafka_topics_ok"] is False
    assert result.data["missing_topics"] == ()
    assert result.data["errored_topics"] == ()
    assert "metadata_timeout_seconds" in result.detail


async def test_make_kafka_topics_check_runs_fetch_off_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    main_thread_id = threading.get_ident()
    worker_thread_ids: list[int] = []

    class _ThreadCapturingAdminClient:
        def __init__(self, _config: dict[str, object]) -> None:
            self._config = _config

        def list_topics(self, timeout: float) -> object:
            _ = timeout
            worker_thread_ids.append(threading.get_ident())
            return _metadata_for({"in", "out"})

    monkeypatch.setattr(readiness_mod, "AdminClient", _ThreadCapturingAdminClient)
    check = make_kafka_topics_check(
        admin_config={"bootstrap.servers": "localhost:9092"},
        required_topics=("in", "out"),
        timeout_seconds=1.0,
    )
    result = await check()
    assert result.ok is True
    assert worker_thread_ids
    assert worker_thread_ids[0] != main_thread_id


async def test_make_kafka_topics_check_reuses_single_inflight_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _CountingSlowAdminClient:
        call_count = 0

        def __init__(self, _config: dict[str, object]) -> None:
            self._config = _config

        def list_topics(self, timeout: float) -> object:
            _ = timeout
            _CountingSlowAdminClient.call_count += 1
            time.sleep(0.05)
            return _metadata_for({"in", "out"})

    monkeypatch.setattr(readiness_mod, "AdminClient", _CountingSlowAdminClient)
    check = make_kafka_topics_check(
        admin_config={"bootstrap.servers": "localhost:9092"},
        required_topics=("in", "out"),
        timeout_seconds=1.0,
    )
    results = await asyncio.gather(check(), check(), check())
    assert all(result.ok for result in results)
    assert _CountingSlowAdminClient.call_count == 1


async def test_readiness_manager_background_loop_and_stop() -> None:
    state = {"ok": False}

    def _toggle() -> bool:
        if not state["ok"]:
            state["ok"] = True
            return False
        return True

    manager = ReadinessManager(
        checks=(
            make_callable_check(
                name="dep",
                check=_toggle,
                reason_when_false="dependency_unavailable",
                detail_when_false="not ready",
            ),
        ),
        interval_seconds=0.01,
    )

    startup_snapshot = await manager.evaluate_once(source="startup")
    assert not startup_snapshot.ready

    await manager.start_background()
    deadline = time.monotonic() + 1.0
    while not manager.snapshot.ready and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    assert manager.snapshot.ready is True

    await manager.start_background()
    await manager.stop_background()
    await manager.stop_background()


async def test_get_check_result_returns_match_or_none() -> None:
    snapshot = ReadinessSnapshot(
        status="degraded",
        ready=False,
        reason="dependency_unavailable",
        detail="not ready",
        last_checked_at=1.0,
        check_results=(
            CheckResult(name="kafka", ok=True),
            CheckResult(name="esri", ok=False),
        ),
    )

    assert readiness_mod.get_check_result(snapshot, "esri") == snapshot.check_results[1]
    assert readiness_mod.get_check_result(snapshot, "missing") is None


async def test_readiness_manager_rejects_empty_checks() -> None:
    with pytest.raises(ValueError, match="At least one readiness check is required"):
        ReadinessManager(checks=(), interval_seconds=1.0)


async def test_readiness_manager_get_check_result_uses_cached_snapshot() -> None:
    async def _check() -> CheckResult:
        return CheckResult(name="dep", ok=True)

    manager = ReadinessManager(
        checks=(_check,),
        interval_seconds=0.01,
    )
    await manager.evaluate_once(source="startup")

    result = manager.get_check_result("dep")

    assert result is not None
    assert result.ok is True


async def test_metadata_fetch_coordinator_resets_inflight_on_success() -> None:
    class _CoordinatorWithoutDoneCallback(readiness_mod._MetadataFetchCoordinator):
        def _make_fetch_task(
            self,
            *,
            admin: AdminClient,
            timeout_seconds: float,
        ) -> asyncio.Task[object]:
            return asyncio.create_task(
                readiness_mod._run_in_daemon_thread_and_wait(
                    admin.list_topics,
                    timeout=timeout_seconds,
                )
            )

    class _Admin:
        def list_topics(self, timeout: float) -> object:
            _ = timeout
            return _metadata_for({"in"})

    coordinator = _CoordinatorWithoutDoneCallback()
    metadata = await coordinator.fetch(
        admin=cast(AdminClient, _Admin()), timeout_seconds=1.0
    )

    assert getattr(metadata, "topics", None) is not None
    assert coordinator._inflight_task is None


async def test_metadata_fetch_coordinator_resets_inflight_on_fetch_failure() -> None:
    class _CoordinatorWithoutDoneCallback(readiness_mod._MetadataFetchCoordinator):
        def _make_fetch_task(
            self,
            *,
            admin: AdminClient,
            timeout_seconds: float,
        ) -> asyncio.Task[object]:
            return asyncio.create_task(
                readiness_mod._run_in_daemon_thread_and_wait(
                    admin.list_topics,
                    timeout=timeout_seconds,
                )
            )

    class _FailingAdmin:
        def list_topics(self, timeout: float) -> object:
            _ = timeout
            raise RuntimeError("broken broker")

    coordinator = _CoordinatorWithoutDoneCallback()

    with pytest.raises(RuntimeError, match="broken broker"):
        await coordinator.fetch(
            admin=cast(AdminClient, _FailingAdmin()),
            timeout_seconds=1.0,
        )

    assert coordinator._inflight_task is None


async def test_readiness_manager_stop_background_cancels_task_after_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = ReadinessManager(
        checks=(
            make_callable_check(
                name="dep",
                check=lambda: True,
                reason_when_false="dependency_unavailable",
                detail_when_false="not ready",
            ),
        ),
        interval_seconds=0.01,
    )

    async def _never_finishes() -> None:
        await asyncio.Event().wait()

    stalled = asyncio.create_task(_never_finishes(), name="stalled-readiness-task")
    manager._task = stalled

    async def _raise_timeout(awaitable: object, timeout: float) -> object:
        _ = (awaitable, timeout)
        raise TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", _raise_timeout)
    await manager.stop_background()

    assert manager._task is None
