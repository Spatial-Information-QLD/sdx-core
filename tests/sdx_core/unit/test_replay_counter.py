from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, cast

import pytest
import pytest_asyncio
from faststream.confluent.fastapi import KafkaMessage

from sdx_core.replay_counter import (
    ReplayKey,
    clear_all_attempts,
    clear_attempt,
    extract_replay_key,
    increment_attempt,
)

pytestmark = pytest.mark.asyncio


@dataclass
class _FakeRawMessage:
    topic_value: str
    partition_value: int
    offset_value: int

    def topic(self) -> str:
        return self.topic_value

    def partition(self) -> int:
        return self.partition_value

    def offset(self) -> int:
        return self.offset_value


class _FakeMessage:
    def __init__(self, raw_message: Any) -> None:
        self.raw_message = raw_message


@pytest_asyncio.fixture(autouse=True)
async def _reset_attempt_state() -> AsyncIterator[None]:
    await clear_all_attempts()
    yield
    await clear_all_attempts()


async def test_extract_replay_key_reads_topic_partition_offset() -> None:
    message = _FakeMessage(
        raw_message=_FakeRawMessage(
            topic_value="input.topic",
            partition_value=2,
            offset_value=17,
        )
    )

    replay_key = extract_replay_key(cast(KafkaMessage, message))

    assert replay_key == ReplayKey(topic="input.topic", partition=2, offset=17)


async def test_extract_replay_key_supports_tuple_raw_message_shape() -> None:
    raw = _FakeRawMessage(topic_value="input.topic", partition_value=1, offset_value=9)
    message = _FakeMessage(raw_message=(raw, object()))

    replay_key = extract_replay_key(cast(KafkaMessage, message))

    assert replay_key == ReplayKey(topic="input.topic", partition=1, offset=9)


async def test_increment_attempt_increases_monotonically_per_key() -> None:
    key = ReplayKey(topic="input.topic", partition=1, offset=5)

    assert await increment_attempt(key) == 1
    assert await increment_attempt(key) == 2
    assert await increment_attempt(key) == 3


async def test_increment_attempt_isolated_across_distinct_keys() -> None:
    key_one = ReplayKey(topic="input.topic", partition=1, offset=5)
    key_two = ReplayKey(topic="input.topic", partition=1, offset=6)

    assert await increment_attempt(key_one) == 1
    assert await increment_attempt(key_two) == 1
    assert await increment_attempt(key_one) == 2
    assert await increment_attempt(key_two) == 2


async def test_clear_attempt_removes_single_key() -> None:
    key_one = ReplayKey(topic="input.topic", partition=1, offset=5)
    key_two = ReplayKey(topic="input.topic", partition=1, offset=6)
    await increment_attempt(key_one)
    await increment_attempt(key_two)

    await clear_attempt(key_one)

    assert await increment_attempt(key_one) == 1
    assert await increment_attempt(key_two) == 2


async def test_clear_all_attempts_resets_global_state() -> None:
    key = ReplayKey(topic="input.topic", partition=1, offset=5)
    await increment_attempt(key)
    await increment_attempt(key)

    await clear_all_attempts()

    assert await increment_attempt(key) == 1
