from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import pytest

from sdx_core.circuit_breaker import CircuitState
from sdx_core.circuit_breaker.integrations.faststream_confluent.runtime import (
    CircuitPauseResumeListener,
    ConfluentAssignmentPauser,
)

pytestmark = pytest.mark.asyncio


@dataclass
class _FakeConfluentConsumer:
    assignments: Sequence[Any]
    pause_calls: int = 0
    resume_calls: int = 0

    def assignment(self) -> Sequence[Any]:
        return self.assignments

    def pause(self, partitions: Sequence[Any]) -> None:
        _ = partitions
        self.pause_calls += 1

    def resume(self, partitions: Sequence[Any]) -> None:
        _ = partitions
        self.resume_calls += 1


@dataclass
class _FakeAsyncConfluentConsumer:
    consumer: _FakeConfluentConsumer
    _thread_pool: None = None


@dataclass
class _FakeSubscriber:
    consumer: _FakeAsyncConfluentConsumer | None


@dataclass
class _RecordingPauser:
    pause_calls: int = 0
    resume_calls: int = 0

    async def pause_assigned(self) -> None:
        self.pause_calls += 1

    async def resume_assigned(self) -> None:
        self.resume_calls += 1


class _BlockingSleep:
    def __init__(self) -> None:
        self.started = 0
        self.cancelled = 0
        self._release = asyncio.Event()

    async def __call__(self, seconds: float) -> None:
        _ = seconds
        self.started += 1
        try:
            await self._release.wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            raise


async def test_pauser_pause_and_resume_assigned_partitions() -> None:
    confluent_consumer = _FakeConfluentConsumer(assignments=("tp-1",))
    subscriber = _FakeSubscriber(
        consumer=_FakeAsyncConfluentConsumer(confluent_consumer)
    )
    pauser = ConfluentAssignmentPauser(subscriber=subscriber)

    await pauser.pause_assigned()
    await pauser.resume_assigned()

    assert confluent_consumer.pause_calls == 1
    assert confluent_consumer.resume_calls == 1


async def test_pauser_noops_when_subscriber_consumer_missing() -> None:
    pauser = ConfluentAssignmentPauser(subscriber=_FakeSubscriber(consumer=None))
    await pauser.pause_assigned()
    await pauser.resume_assigned()


async def test_pauser_noops_for_non_subscriber_object() -> None:
    pauser = ConfluentAssignmentPauser(subscriber=object())
    await pauser.pause_assigned()
    await pauser.resume_assigned()


async def test_pauser_pause_noops_without_assignments() -> None:
    confluent_consumer = _FakeConfluentConsumer(assignments=())
    subscriber = _FakeSubscriber(
        consumer=_FakeAsyncConfluentConsumer(confluent_consumer)
    )
    pauser = ConfluentAssignmentPauser(subscriber=subscriber)

    await pauser.pause_assigned()

    assert confluent_consumer.pause_calls == 0


async def test_pauser_pause_is_idempotent_when_already_paused() -> None:
    confluent_consumer = _FakeConfluentConsumer(assignments=("tp-1",))
    subscriber = _FakeSubscriber(
        consumer=_FakeAsyncConfluentConsumer(confluent_consumer)
    )
    pauser = ConfluentAssignmentPauser(subscriber=subscriber)

    await pauser.pause_assigned()
    await pauser.pause_assigned()

    assert confluent_consumer.pause_calls == 1


async def test_pauser_resume_clears_paused_flag_when_consumer_becomes_missing() -> None:
    confluent_consumer = _FakeConfluentConsumer(assignments=("tp-1",))
    subscriber = _FakeSubscriber(
        consumer=_FakeAsyncConfluentConsumer(confluent_consumer)
    )
    pauser = ConfluentAssignmentPauser(subscriber=subscriber)

    await pauser.pause_assigned()
    subscriber.consumer = None
    await pauser.resume_assigned()
    await pauser.resume_assigned()

    assert confluent_consumer.resume_calls == 0


async def test_listener_open_pauses_and_schedules_resume() -> None:
    pauser = _RecordingPauser()
    listener = CircuitPauseResumeListener(
        pauser=pauser,
        recovery_timeout_seconds=0.0,
    )

    await listener.on_state_change("esri", CircuitState.CLOSED, CircuitState.OPEN)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert pauser.pause_calls == 1
    assert pauser.resume_calls == 1


async def test_listener_reopen_cancels_previous_resume_task() -> None:
    pauser = _RecordingPauser()
    blocking_sleep = _BlockingSleep()
    listener = CircuitPauseResumeListener(
        pauser=pauser,
        recovery_timeout_seconds=30.0,
        sleep=blocking_sleep,
    )

    await listener.on_state_change("esri", CircuitState.CLOSED, CircuitState.OPEN)
    await listener.on_state_change("esri", CircuitState.OPEN, CircuitState.OPEN)
    await asyncio.sleep(0)

    await listener.close()

    assert pauser.pause_calls == 2
    assert blocking_sleep.started >= 1
    assert blocking_sleep.cancelled >= 1


async def test_listener_half_open_to_closed_resumes_and_cancels_task() -> None:
    pauser = _RecordingPauser()
    blocking_sleep = _BlockingSleep()
    listener = CircuitPauseResumeListener(
        pauser=pauser,
        recovery_timeout_seconds=30.0,
        sleep=blocking_sleep,
    )

    await listener.on_state_change("esri", CircuitState.CLOSED, CircuitState.OPEN)
    await asyncio.sleep(0)
    await listener.on_state_change(
        "esri",
        CircuitState.HALF_OPEN,
        CircuitState.CLOSED,
    )
    await asyncio.sleep(0)

    assert pauser.pause_calls == 1
    assert pauser.resume_calls == 1
    assert blocking_sleep.cancelled >= 1


async def test_listener_noop_callbacks_and_close_without_task() -> None:
    pauser = _RecordingPauser()
    listener = CircuitPauseResumeListener(
        pauser=pauser,
        recovery_timeout_seconds=0.1,
    )

    await listener.on_call_rejected("svc")
    await listener.on_call_succeeded("svc", 0.1)
    await listener.on_call_failed("svc", RuntimeError("boom"), 0.2)
    await listener.close()
