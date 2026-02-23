from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass

from tenacity import (
    AsyncRetrying,
    RetryCallState,
    stop_after_attempt,
    stop_never,
    wait_exponential_jitter,
)
from tenacity.retry import retry_base


@dataclass(frozen=True)
class RetryBackoffPolicy:
    """Configuration for retry attempt count and backoff boundaries."""

    attempts: int | None
    min_seconds: float
    max_seconds: float

    def __post_init__(self) -> None:
        if self.attempts is not None and self.attempts < 1:
            raise ValueError("attempts must be >= 1 when provided")
        if self.min_seconds < 0:
            raise ValueError("min_seconds must be >= 0")
        if self.max_seconds < 0:
            raise ValueError("max_seconds must be >= 0")
        if self.max_seconds < self.min_seconds:
            raise ValueError("max_seconds must be >= min_seconds")


def build_interruptible_sleep(
    stop_event: asyncio.Event,
) -> Callable[[float], Awaitable[None]]:
    """Build an async sleep that exits early when shutdown is requested."""

    async def _interruptible_sleep(delay: float) -> None:
        if stop_event.is_set():
            return

        bounded_delay = max(delay, 0.0)
        with suppress(TimeoutError):
            await asyncio.wait_for(stop_event.wait(), timeout=bounded_delay)

    return _interruptible_sleep


def build_exponential_jitter_retrying(
    *,
    retry: retry_base,
    policy: RetryBackoffPolicy,
    sleep: Callable[[float], Awaitable[None]] | None = None,
    before_sleep: Callable[[RetryCallState], None] | None = None,
    reraise: bool = True,
) -> AsyncRetrying:
    """Build an ``AsyncRetrying`` with exponential jitter backoff."""
    stop = (
        stop_never if policy.attempts is None else stop_after_attempt(policy.attempts)
    )
    wait = wait_exponential_jitter(
        initial=policy.min_seconds,
        max=policy.max_seconds,
    )
    if sleep is None and before_sleep is None:
        return AsyncRetrying(
            retry=retry,
            wait=wait,
            stop=stop,
            reraise=reraise,
        )
    if sleep is None:
        return AsyncRetrying(
            retry=retry,
            wait=wait,
            stop=stop,
            before_sleep=before_sleep,
            reraise=reraise,
        )
    if before_sleep is None:
        return AsyncRetrying(
            retry=retry,
            wait=wait,
            stop=stop,
            sleep=sleep,
            reraise=reraise,
        )
    return AsyncRetrying(
        retry=retry,
        wait=wait,
        stop=stop,
        sleep=sleep,
        before_sleep=before_sleep,
        reraise=reraise,
    )
