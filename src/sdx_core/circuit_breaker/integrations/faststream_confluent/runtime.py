from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from concurrent.futures import Executor
from contextlib import suppress
from functools import partial
from typing import Any, Protocol, runtime_checkable

from sdx_core.circuit_breaker import BreakerListener, CircuitState


class _ConfluentConsumerLike(Protocol):
    """Subset of confluent consumer methods required for pause/resume."""

    def assignment(self) -> Sequence[Any]:
        """Return currently assigned partitions."""

    def pause(self, partitions: Sequence[Any]) -> None:
        """Pause consuming the provided partitions."""

    def resume(self, partitions: Sequence[Any]) -> None:
        """Resume consuming the provided partitions."""


class _AsyncConfluentConsumerLike(Protocol):
    """Subset of FastStream AsyncConfluentConsumer used by this service."""

    consumer: _ConfluentConsumerLike
    _thread_pool: Executor | None


@runtime_checkable
class _SubscriberLike(Protocol):
    """Minimal subscriber surface needed to access the consumer wrapper."""

    consumer: _AsyncConfluentConsumerLike | None


class _PauserLike(Protocol):
    """Pause/resume adapter interface used by listener orchestration."""

    async def pause_assigned(self) -> None:
        """Pause assigned partitions."""

    async def resume_assigned(self) -> None:
        """Resume assigned partitions."""


class ConfluentAssignmentPauser:
    """Pause and resume assigned partitions for one FastStream subscriber."""

    def __init__(self, *, subscriber: object) -> None:
        """Create a pauser bound to one subscriber instance.

        Args:
            subscriber: FastStream subscriber wrapper holding the Kafka consumer.
        """
        self._subscriber = subscriber
        self._lock = asyncio.Lock()
        self._paused = False

    async def pause_assigned(self) -> None:
        """Pause current partition assignments when consumer is available."""
        async with self._lock:
            consumer = self._resolve_consumer()
            if consumer is None:
                return
            if self._paused:
                return

            loop = asyncio.get_running_loop()
            assignments = await loop.run_in_executor(
                consumer._thread_pool,
                consumer.consumer.assignment,
            )
            if not assignments:
                return

            pause_assigned = partial(consumer.consumer.pause, assignments)
            await loop.run_in_executor(
                consumer._thread_pool,
                pause_assigned,
            )
            self._paused = True

    async def resume_assigned(self) -> None:
        """Resume current partition assignments when paused."""
        async with self._lock:
            if not self._paused:
                return

            consumer = self._resolve_consumer()
            if consumer is None:
                self._paused = False
                return

            loop = asyncio.get_running_loop()
            assignments = await loop.run_in_executor(
                consumer._thread_pool,
                consumer.consumer.assignment,
            )
            if assignments:
                resume_assigned = partial(consumer.consumer.resume, assignments)
                await loop.run_in_executor(
                    consumer._thread_pool,
                    resume_assigned,
                )
            self._paused = False

    def _resolve_consumer(self) -> _AsyncConfluentConsumerLike | None:
        subscriber = self._subscriber
        if not isinstance(subscriber, _SubscriberLike):
            return None
        return subscriber.consumer


class CircuitPauseResumeListener(BreakerListener):
    """Listener that pauses on OPEN and schedules resume for probe windows."""

    def __init__(
        self,
        *,
        pauser: _PauserLike,
        recovery_timeout_seconds: float,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        """Create pause/resume orchestration for circuit transitions.

        Args:
            pauser: Adapter that pauses or resumes assigned partitions.
            recovery_timeout_seconds: Delay before automatic resume for probe
                windows.
            sleep: Awaitable sleep function used for delayed resume scheduling.
        """
        self._pauser = pauser
        self._recovery_timeout_seconds = recovery_timeout_seconds
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._resume_task: asyncio.Task[None] | None = None

    async def on_state_change(
        self,
        name: str,
        old: CircuitState,
        new: CircuitState,
    ) -> None:
        """Handle breaker state transitions for pause/resume orchestration."""
        _ = name
        if new == CircuitState.OPEN:
            await self._pause_and_schedule_resume()
            return
        if old == CircuitState.HALF_OPEN and new == CircuitState.CLOSED:
            await self._cancel_resume_and_resume_now()

    async def on_call_rejected(self, name: str) -> None:
        """No-op for this listener."""
        _ = name

    async def on_call_succeeded(self, name: str, elapsed: float) -> None:
        """No-op for this listener."""
        _ = (name, elapsed)

    async def on_call_failed(self, name: str, exc: Exception, elapsed: float) -> None:
        """No-op for this listener."""
        _ = (name, exc, elapsed)

    async def close(self) -> None:
        """Cancel any scheduled resume task."""
        task: asyncio.Task[None] | None
        async with self._lock:
            task = self._resume_task
            self._resume_task = None
            if task is not None:
                task.cancel()
        if task is None:
            return
        with suppress(asyncio.CancelledError):
            await task

    async def _pause_and_schedule_resume(self) -> None:
        async with self._lock:
            await self._pauser.pause_assigned()
            if self._resume_task is not None:
                self._resume_task.cancel()
            self._resume_task = asyncio.create_task(
                self._resume_after_timeout(),
                name="circuit_breaker_resume",
            )

    async def _cancel_resume_and_resume_now(self) -> None:
        async with self._lock:
            if self._resume_task is not None:
                self._resume_task.cancel()
                self._resume_task = None
            await self._pauser.resume_assigned()

    async def _resume_after_timeout(self) -> None:
        try:
            await self._sleep(self._recovery_timeout_seconds)
        except asyncio.CancelledError:
            return

        async with self._lock:
            await self._pauser.resume_assigned()
            self._resume_task = None
