from __future__ import annotations

import asyncio

import pytest
from tenacity import AsyncRetrying, RetryCallState, RetryError
from tenacity.retry import retry_if_exception_type

from sdx_core.retry import (
    RetryBackoffPolicy,
    build_exponential_jitter_retrying,
    build_interruptible_sleep,
)

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    ("attempts", "min_seconds", "max_seconds", "message"),
    [
        (0, 0.0, 1.0, "attempts must be >= 1"),
        (1, -0.1, 1.0, "min_seconds must be >= 0"),
        (1, 0.1, -0.1, "max_seconds must be >= 0"),
        (1, 2.0, 1.0, "max_seconds must be >= min_seconds"),
    ],
)
async def test_retry_backoff_policy_validation(
    attempts: int,
    min_seconds: float,
    max_seconds: float,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        RetryBackoffPolicy(
            attempts=attempts,
            min_seconds=min_seconds,
            max_seconds=max_seconds,
        )


async def test_interruptible_sleep_returns_immediately_when_stop_event_is_set() -> None:
    stop_event = asyncio.Event()
    stop_event.set()
    sleep = build_interruptible_sleep(stop_event)

    await asyncio.wait_for(sleep(30.0), timeout=0.1)


async def test_interruptible_sleep_waits_for_delay_when_not_interrupted() -> None:
    stop_event = asyncio.Event()
    sleep = build_interruptible_sleep(stop_event)

    await asyncio.wait_for(sleep(0.01), timeout=0.2)


async def test_build_retrying_without_optional_hooks() -> None:
    retrying = build_exponential_jitter_retrying(
        retry=retry_if_exception_type(ValueError),
        policy=RetryBackoffPolicy(attempts=2, min_seconds=0.0, max_seconds=0.0),
    )

    assert isinstance(retrying, AsyncRetrying)


async def test_build_retrying_with_before_sleep_only() -> None:
    before_sleep_calls: list[int] = []

    def _before_sleep(state: RetryCallState) -> None:
        attempt_number = state.attempt_number
        assert attempt_number is not None
        before_sleep_calls.append(attempt_number)

    retrying = build_exponential_jitter_retrying(
        retry=retry_if_exception_type(ValueError),
        policy=RetryBackoffPolicy(attempts=3, min_seconds=0.0, max_seconds=0.0),
        before_sleep=_before_sleep,
    )

    attempts = 0
    with pytest.raises(ValueError):
        async for attempt in retrying:
            with attempt:
                attempts += 1
                raise ValueError("boom")

    assert attempts == 3
    assert before_sleep_calls == [1, 2]


async def test_build_retrying_with_sleep_only() -> None:
    sleep_calls: list[float] = []

    async def _sleep(delay: float) -> None:
        sleep_calls.append(delay)

    retrying = build_exponential_jitter_retrying(
        retry=retry_if_exception_type(ValueError),
        policy=RetryBackoffPolicy(attempts=None, min_seconds=0.0, max_seconds=0.0),
        sleep=_sleep,
    )

    attempts = 0
    async for attempt in retrying:
        with attempt:
            attempts += 1
            if attempts < 4:
                raise ValueError("retry")

    assert attempts == 4
    assert len(sleep_calls) == 3


async def test_build_retrying_with_sleep_and_before_sleep_and_reraise_disabled() -> (
    None
):
    before_sleep_calls: list[int] = []
    sleep_calls: list[float] = []

    async def _sleep(delay: float) -> None:
        sleep_calls.append(delay)

    def _before_sleep(state: RetryCallState) -> None:
        attempt_number = state.attempt_number
        assert attempt_number is not None
        before_sleep_calls.append(attempt_number)

    retrying = build_exponential_jitter_retrying(
        retry=retry_if_exception_type(ValueError),
        policy=RetryBackoffPolicy(attempts=2, min_seconds=0.0, max_seconds=0.0),
        sleep=_sleep,
        before_sleep=_before_sleep,
        reraise=False,
    )

    with pytest.raises(RetryError):
        async for attempt in retrying:
            with attempt:
                raise ValueError("boom")

    assert before_sleep_calls == [1]
    assert len(sleep_calls) == 1
