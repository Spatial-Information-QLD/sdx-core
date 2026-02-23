from __future__ import annotations

import logging
import sys
from collections.abc import Callable
from typing import Protocol, cast

import pytest
import structlog
from faststream import ContextRepo

from sdx_core.logging import (
    configure_structlog,
    get_log_level_value,
    log_error,
    log_exception,
    log_info,
    log_warning,
)


def _configured_renderer() -> object:
    root_handler = logging.getLogger().handlers[0]
    formatter = root_handler.formatter
    assert isinstance(formatter, structlog.stdlib.ProcessorFormatter)
    return formatter.processors[-1]


def test_get_log_level_value_maps_known_levels() -> None:
    assert get_log_level_value("debug") == logging.DEBUG
    assert get_log_level_value("INFO") == logging.INFO
    assert get_log_level_value(" warning ") == logging.WARNING
    assert get_log_level_value("ERROR") == logging.ERROR
    assert get_log_level_value("critical") == logging.CRITICAL


def test_get_log_level_value_rejects_unknown_level() -> None:
    with pytest.raises(ValueError):
        get_log_level_value("TRACE")


def test_configure_structlog_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys.stderr, "isatty", lambda: False, raising=False)
    context = ContextRepo()

    first_logger = configure_structlog(log_level="INFO", context=context)
    second_logger = configure_structlog(log_level="DEBUG", context=context)

    assert len(logging.getLogger().handlers) == 1
    assert logging.getLogger().level == logging.DEBUG
    assert first_logger is not None
    assert second_logger is not None


def test_configure_structlog_uses_console_renderer_for_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys.stderr, "isatty", lambda: True, raising=False)

    configure_structlog(log_level="INFO", context=ContextRepo())
    renderer = _configured_renderer()

    assert isinstance(renderer, structlog.dev.ConsoleRenderer)


def test_configure_structlog_uses_json_renderer_for_non_tty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys.stderr, "isatty", lambda: False, raising=False)

    configure_structlog(log_level="INFO", context=ContextRepo())
    renderer = _configured_renderer()

    assert isinstance(renderer, structlog.processors.JSONRenderer)


class _CaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class _FakeStructuredLogger:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def info(self, event: str, **kwargs: object) -> None:
        self.calls.append(("info", event, dict(kwargs)))

    def warning(self, event: str, **kwargs: object) -> None:
        self.calls.append(("warning", event, dict(kwargs)))

    def error(self, event: str, **kwargs: object) -> None:
        self.calls.append(("error", event, dict(kwargs)))

    def exception(self, event: str, **kwargs: object) -> None:
        self.calls.append(("exception", event, dict(kwargs)))


class _RecordWithStructuredFields(Protocol):
    topic: str
    offset: int


@pytest.mark.parametrize(
    ("log_fn", "level"),
    [
        (log_info, "info"),
        (log_warning, "warning"),
        (log_error, "error"),
        (log_exception, "exception"),
    ],
)
def test_structured_log_helpers_forward_keyword_fields(
    log_fn: Callable[..., None],
    level: str,
) -> None:
    logger = _FakeStructuredLogger()

    log_fn(logger, "message.event", topic="topic-a", attempt=3)

    assert logger.calls == [
        (
            level,
            "message.event",
            {"topic": "topic-a", "attempt": 3},
        )
    ]


def test_structured_log_helpers_support_stdlib_logger_extra() -> None:
    logger = logging.getLogger("tests.sdx_core.logging.helpers")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(logging.INFO)
    handler = _CaptureHandler()
    logger.addHandler(handler)

    log_info(logger, "message.received", topic="topic-a", offset=42)

    assert len(handler.records) == 1
    record = handler.records[0]
    typed_record = cast(_RecordWithStructuredFields, record)
    assert record.getMessage() == "message.received"
    assert typed_record.topic == "topic-a"
    assert typed_record.offset == 42
