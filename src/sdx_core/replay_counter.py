from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol, cast

from faststream.confluent.fastapi import KafkaMessage


@dataclass(frozen=True)
class ReplayKey:
    """In-memory replay key for one consumed Kafka record."""

    topic: str
    partition: int
    offset: int


class _ReplayRawMessage(Protocol):
    """Raw Kafka message surface required for replay-key extraction."""

    def topic(self) -> str:
        """Return source topic name."""

    def partition(self) -> int:
        """Return source partition."""

    def offset(self) -> int:
        """Return source offset."""


_REPLAY_ATTEMPTS: dict[ReplayKey, int] = {}
_REPLAY_ATTEMPTS_LOCK = asyncio.Lock()


def extract_replay_key(message: KafkaMessage) -> ReplayKey:
    """Extract replay key tuple `(topic, partition, offset)` from a message."""
    raw_message = message.raw_message
    if isinstance(raw_message, tuple):
        raw_message = raw_message[0]
    raw = cast(_ReplayRawMessage, raw_message)
    return ReplayKey(
        topic=raw.topic(),
        partition=raw.partition(),
        offset=raw.offset(),
    )


async def increment_attempt(key: ReplayKey) -> int:
    """Increment and return replay attempts for one message key."""
    async with _REPLAY_ATTEMPTS_LOCK:
        next_attempt = _REPLAY_ATTEMPTS.get(key, 0) + 1
        _REPLAY_ATTEMPTS[key] = next_attempt
        return next_attempt


async def clear_attempt(key: ReplayKey) -> None:
    """Remove replay-attempt state for one message key."""
    async with _REPLAY_ATTEMPTS_LOCK:
        _REPLAY_ATTEMPTS.pop(key, None)


async def clear_all_attempts() -> None:
    """Clear all replay-attempt state. Intended for deterministic tests."""
    async with _REPLAY_ATTEMPTS_LOCK:
        _REPLAY_ATTEMPTS.clear()
