from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, cast

from confluent_kafka.admin import AdminClient

REASON_STARTING = "starting"
REASON_READY = "ready"
REASON_DEPENDENCY_UNAVAILABLE = "dependency_unavailable"
REASON_CHECK_FAILED = "check_failed"
_logger = logging.getLogger(__name__)

ReadinessCheck = Callable[[], Awaitable["CheckResult"]]
BoolCheck = Callable[[], bool] | Callable[[], Awaitable[bool]]


@dataclass(frozen=True)
class CheckResult:
    """Result of one dependency readiness check."""

    name: str
    ok: bool
    reason: str | None = None
    detail: str = ""
    data: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Freeze check metadata mapping to keep snapshots read-only."""
        frozen_data = MappingProxyType(dict(self.data))
        object.__setattr__(self, "data", frozen_data)


@dataclass(frozen=True)
class ReadinessSnapshot:
    """Immutable snapshot of readiness and per-check outcomes."""

    status: str
    ready: bool
    reason: str
    detail: str
    last_checked_at: float
    check_results: tuple[CheckResult, ...]


class SnapshotCallback(Protocol):
    """Callback fired whenever a readiness snapshot is refreshed."""

    def __call__(
        self,
        previous: ReadinessSnapshot | None,
        current: ReadinessSnapshot,
        *,
        source: str,
    ) -> None:
        """Handle a readiness snapshot transition."""


class _MetadataFetchTimeoutError(TimeoutError):
    """Raised when Kafka metadata fetch exceeds readiness timeout."""

    def __init__(self, timeout_seconds: float) -> None:
        self.timeout_seconds = timeout_seconds
        super().__init__(f"metadata_timeout_seconds={timeout_seconds:g}")


class _MetadataFetchCoordinator:
    """Coordinate at most one in-flight metadata fetch across concurrent checks."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._inflight_task: asyncio.Task[object] | None = None

    def _make_fetch_task(
        self,
        *,
        admin: AdminClient,
        timeout_seconds: float,
    ) -> asyncio.Task[object]:
        task = asyncio.create_task(
            _run_in_daemon_thread_and_wait(
                admin.list_topics,
                timeout=timeout_seconds,
            ),
            name="readiness-metadata-fetch",
        )
        task.add_done_callback(self._on_fetch_task_done)
        return task

    def _on_fetch_task_done(self, task: asyncio.Task[object]) -> None:
        if self._inflight_task is task:
            self._inflight_task = None
        with suppress(asyncio.CancelledError, Exception):
            task.exception()

    async def fetch(
        self,
        *,
        admin: AdminClient,
        timeout_seconds: float,
    ) -> object:
        async with self._lock:
            if self._inflight_task is None:
                self._inflight_task = self._make_fetch_task(
                    admin=admin,
                    timeout_seconds=timeout_seconds,
                )
            task = self._inflight_task

        assert task is not None
        try:
            metadata = await asyncio.wait_for(
                asyncio.shield(task),
                timeout=timeout_seconds,
            )
        except TimeoutError as exc:
            raise _MetadataFetchTimeoutError(timeout_seconds) from exc
        except Exception:
            async with self._lock:
                if self._inflight_task is task:
                    self._inflight_task = None
            raise

        async with self._lock:
            if self._inflight_task is task:
                self._inflight_task = None
        return metadata


async def _run_in_daemon_thread_and_wait(
    func: Callable[..., object],
    /,
    *args: object,
    **kwargs: object,
) -> object:
    """Run a blocking callable in a daemon thread and await completion."""
    done = threading.Event()
    error: Exception | None = None
    result: object | None = None

    def _run() -> None:
        nonlocal error, result
        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            error = exc
        finally:
            done.set()

    thread = threading.Thread(target=_run, name="readiness-metadata-fetch", daemon=True)
    thread.start()
    while not done.is_set():
        await asyncio.sleep(0.005)
    if error is not None:
        raise error
    return result


def build_starting_snapshot(
    check_names: Sequence[str],
    *,
    detail: str = "Readiness checks have not completed yet.",
    now_fn: Callable[[], float] = time.time,
) -> ReadinessSnapshot:
    """Build default snapshot before any checks have been executed."""
    return ReadinessSnapshot(
        status="degraded",
        ready=False,
        reason=REASON_STARTING,
        detail=detail,
        last_checked_at=now_fn(),
        check_results=tuple(
            CheckResult(
                name=name,
                ok=False,
                reason=REASON_STARTING,
                detail=detail,
            )
            for name in check_names
        ),
    )


def get_check_result(snapshot: ReadinessSnapshot, name: str) -> CheckResult | None:
    """Return one check result by name from a readiness snapshot."""
    for result in snapshot.check_results:
        if result.name == name:
            return result
    return None


async def _resolve_bool_check(check: BoolCheck) -> bool:
    result = check()
    if inspect.isawaitable(result):
        awaited = await cast(Awaitable[object], result)
        return bool(awaited)
    return bool(result)


def make_callable_check(
    *,
    name: str,
    check: BoolCheck,
    reason_when_false: str,
    detail_when_false: str,
    true_detail: str = "",
) -> ReadinessCheck:
    """Build a readiness check from a sync/async boolean callable."""

    async def _check() -> CheckResult:
        try:
            ok = await _resolve_bool_check(check)
        except Exception as exc:
            return CheckResult(
                name=name,
                ok=False,
                reason=REASON_CHECK_FAILED,
                detail=f"{exc.__class__.__name__}: {exc}",
            )
        if ok:
            return CheckResult(name=name, ok=True, detail=true_detail)
        return CheckResult(
            name=name,
            ok=False,
            reason=reason_when_false,
            detail=detail_when_false,
        )

    _check.__name__ = name
    return _check


def make_kafka_topics_check(
    *,
    admin_config: Mapping[str, str | int | float | bool],
    required_topics: Sequence[str],
    timeout_seconds: float,
    name: str = "kafka",
    reason_unreachable: str = "kafka_unreachable",
    reason_topics: str = "topic_missing",
) -> ReadinessCheck:
    """Build a Kafka readiness check using topic metadata from AdminClient."""
    admin = AdminClient(dict(admin_config))
    coordinator = _MetadataFetchCoordinator()
    required = tuple(sorted(set(required_topics)))

    async def _check() -> CheckResult:
        try:
            metadata_obj = await coordinator.fetch(
                admin=admin,
                timeout_seconds=timeout_seconds,
            )
        except _MetadataFetchTimeoutError as exc:
            return CheckResult(
                name=name,
                ok=False,
                reason=reason_unreachable,
                detail=str(exc),
                data={
                    "kafka_broker_ok": False,
                    "kafka_topics_ok": False,
                    "missing_topics": (),
                    "errored_topics": (),
                },
            )
        except Exception as exc:
            return CheckResult(
                name=name,
                ok=False,
                reason=reason_unreachable,
                detail=f"{exc.__class__.__name__}: {exc}",
                data={
                    "kafka_broker_ok": False,
                    "kafka_topics_ok": False,
                    "missing_topics": (),
                    "errored_topics": (),
                },
            )

        topics_obj = getattr(metadata_obj, "topics", {})
        topics = cast(dict[str, object], topics_obj)
        missing: list[str] = []
        errored: list[str] = []
        for topic in required:
            topic_metadata = topics.get(topic)
            if topic_metadata is None:
                missing.append(topic)
                continue
            if getattr(topic_metadata, "error", None) is not None:
                errored.append(topic)

        if missing or errored:
            detail_parts: list[str] = []
            if missing:
                detail_parts.append(f"missing={','.join(missing)}")
            if errored:
                detail_parts.append(f"errored={','.join(errored)}")
            return CheckResult(
                name=name,
                ok=False,
                reason=reason_topics,
                detail=" ".join(detail_parts),
                data={
                    "kafka_broker_ok": True,
                    "kafka_topics_ok": False,
                    "missing_topics": tuple(missing),
                    "errored_topics": tuple(errored),
                },
            )

        return CheckResult(
            name=name,
            ok=True,
            data={
                "kafka_broker_ok": True,
                "kafka_topics_ok": True,
                "missing_topics": (),
                "errored_topics": (),
            },
        )

    _check.__name__ = name
    return _check


async def evaluate_readiness_once(
    *,
    checks: Sequence[ReadinessCheck],
    previous: ReadinessSnapshot | None,
    source: str,
    now_fn: Callable[[], float] = time.time,
    on_snapshot: SnapshotCallback | None = None,
) -> ReadinessSnapshot:
    """Evaluate all readiness checks once and return a new snapshot.

    Snapshot callback errors are logged and suppressed so readiness
    evaluations continue to run.
    """
    results: list[CheckResult] = []
    for check in checks:
        try:
            result = await check()
            results.append(result)
        except Exception as exc:
            check_name = getattr(check, "__name__", "unnamed_check")
            results.append(
                CheckResult(
                    name=check_name,
                    ok=False,
                    reason=REASON_CHECK_FAILED,
                    detail=f"{exc.__class__.__name__}: {exc}",
                )
            )

    ready = all(result.ok for result in results)
    reason = REASON_READY
    detail = ""
    if not ready:
        first_failure = next(result for result in results if not result.ok)
        reason = first_failure.reason or REASON_DEPENDENCY_UNAVAILABLE
        detail = first_failure.detail

    snapshot = ReadinessSnapshot(
        status="ok" if ready else "degraded",
        ready=ready,
        reason=reason,
        detail=detail,
        last_checked_at=now_fn(),
        check_results=tuple(results),
    )
    if on_snapshot is not None:
        callback_name = getattr(on_snapshot, "__name__", on_snapshot.__class__.__name__)
        try:
            on_snapshot(previous, snapshot, source=source)
        except Exception:
            _logger.warning(
                "Readiness snapshot callback failed; continuing",
                exc_info=True,
                extra={
                    "source": source,
                    "ready": snapshot.ready,
                    "reason": snapshot.reason,
                    "callback": callback_name,
                },
            )
    return snapshot


async def run_readiness_loop(
    *,
    evaluate_once: Callable[[str], Awaitable[object]],
    stop_event: asyncio.Event,
    interval_seconds: float,
    source: str = "background",
) -> None:
    """Run periodic readiness checks until shutdown is requested."""
    interval = max(interval_seconds, 0.01)
    while not stop_event.is_set():
        await evaluate_once(source)
        with suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=interval)


class ReadinessManager:
    """Manage cached readiness state with startup and background evaluations."""

    def __init__(
        self,
        *,
        checks: Sequence[ReadinessCheck],
        interval_seconds: float,
        on_snapshot: SnapshotCallback | None = None,
    ) -> None:
        """Initialize readiness manager state and polling configuration.

        Args:
            checks: Readiness checks evaluated on each cycle.
            interval_seconds: Background polling interval in seconds.
            on_snapshot: Optional callback invoked after each evaluation.

        Raises:
            ValueError: If no checks are provided.
        """
        resolved_checks = tuple(checks)
        if not resolved_checks:
            raise ValueError("At least one readiness check is required.")
        self._checks = resolved_checks
        self._interval_seconds = max(interval_seconds, 0.01)
        self._on_snapshot = on_snapshot
        check_names = tuple(
            getattr(check, "__name__", "unnamed_check") for check in resolved_checks
        )
        self._snapshot = build_starting_snapshot(check_names)
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def snapshot(self) -> ReadinessSnapshot:
        """Return the latest cached readiness snapshot."""
        return self._snapshot

    def get_check_result(self, name: str) -> CheckResult | None:
        """Return one named check result from the cached snapshot."""
        return get_check_result(self._snapshot, name)

    async def evaluate_once(self, *, source: str) -> ReadinessSnapshot:
        """Evaluate all checks once and update the cached snapshot."""
        snapshot = await evaluate_readiness_once(
            checks=self._checks,
            previous=self._snapshot,
            source=source,
            on_snapshot=self._on_snapshot,
        )
        self._snapshot = snapshot
        return snapshot

    async def _background_loop(self) -> None:
        await run_readiness_loop(
            evaluate_once=lambda source: self.evaluate_once(source=source),
            stop_event=self._stop_event,
            interval_seconds=self._interval_seconds,
        )

    async def start_background(self) -> None:
        """Start background readiness checks if not already running."""
        if self._task is not None and not self._task.done():
            return
        self._stop_event = asyncio.Event()
        self._task = asyncio.create_task(
            self._background_loop(),
            name="readiness-manager",
        )

    async def stop_background(self) -> None:
        """Stop background readiness checks and await task completion."""
        self._stop_event.set()
        task = self._task
        if task is None:
            return
        grace_seconds = max(self._interval_seconds, 0.01) + 5.0
        try:
            await asyncio.wait_for(task, timeout=grace_seconds)
        except TimeoutError:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        self._task = None
