from __future__ import annotations

import logging
import sys
from collections.abc import Mapping
from typing import Literal, Protocol

import structlog
from faststream import ContextRepo
from structlog.typing import EventDict

_LOG_LEVELS: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_StdlibLogger = logging.Logger | logging.LoggerAdapter[logging.Logger]


class StructuredLogger(Protocol):
    """Logger protocol for structured event logging with keyword fields."""

    def info(self, event: str, **kwargs: object) -> None:
        """Log an informational event."""

    def warning(self, event: str, **kwargs: object) -> None:
        """Log a warning event."""

    def error(self, event: str, **kwargs: object) -> None:
        """Log an error event."""

    def exception(self, event: str, **kwargs: object) -> None:
        """Log an exception event."""


def get_log_level_value(level: str) -> int:
    """Return stdlib log level constant for a normalized level string."""
    normalized = level.strip().upper()
    try:
        return _LOG_LEVELS[normalized]
    except KeyError as error:
        choices = ", ".join(sorted(_LOG_LEVELS))
        raise ValueError(f"log_level must be one of: {choices}") from error


def _select_renderer() -> structlog.types.Processor:
    if sys.stderr.isatty():
        return structlog.dev.ConsoleRenderer()
    return structlog.processors.JSONRenderer()


def _build_faststream_context_merger(
    context: ContextRepo | None,
) -> structlog.types.Processor:
    def _merge_faststream_log_context(
        _: object,
        __: str,
        event_dict: EventDict,
    ) -> EventDict:
        if context is None:
            return event_dict

        try:
            local_context = context.get_local("log_context")
        except LookupError:
            return event_dict

        if not isinstance(local_context, Mapping):
            return event_dict

        merged_extra: dict[str, object] = {}
        extra = event_dict.get("extra")
        if isinstance(extra, Mapping):
            merged_extra.update(dict(extra))
        merged_extra.update({str(key): value for key, value in local_context.items()})
        event_dict["extra"] = merged_extra
        return event_dict

    return _merge_faststream_log_context


def _log(
    logger: StructuredLogger | _StdlibLogger,
    level: Literal["info", "warning", "error", "exception"],
    event: str,
    **fields: object,
) -> None:
    """Log one event across structlog and stdlib logger implementations."""
    method = getattr(logger, level)
    if isinstance(logger, (logging.Logger, logging.LoggerAdapter)):
        method(event, extra=fields)
        return
    method(event, **fields)


def log_info(
    logger: StructuredLogger | _StdlibLogger,
    event: str,
    **fields: object,
) -> None:
    """Log an informational event."""
    _log(logger, "info", event, **fields)


def log_warning(
    logger: StructuredLogger | _StdlibLogger,
    event: str,
    **fields: object,
) -> None:
    """Log a warning event."""
    _log(logger, "warning", event, **fields)


def log_error(
    logger: StructuredLogger | _StdlibLogger,
    event: str,
    **fields: object,
) -> None:
    """Log an error event."""
    _log(logger, "error", event, **fields)


def log_exception(
    logger: StructuredLogger | _StdlibLogger,
    event: str,
    **fields: object,
) -> None:
    """Log an exception event."""
    _log(logger, "exception", event, **fields)


def configure_structlog(
    *,
    log_level: str,
    context: ContextRepo | None = None,
) -> structlog.stdlib.BoundLogger:
    """Configure structlog + stdlib logging for FastStream microservices."""
    level_value = get_log_level_value(log_level)
    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    merge_faststream_context = _build_faststream_context_merger(context)
    renderer = _select_renderer()

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        merge_faststream_context,
        structlog.stdlib.add_log_level,
        timestamper,
    ]

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    logging.basicConfig(
        format="%(message)s",
        handlers=[handler],
        level=level_value,
        force=True,
    )

    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.contextvars.merge_contextvars,
            merge_faststream_context,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            timestamper,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    return structlog.stdlib.get_logger()
