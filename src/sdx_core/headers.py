from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

MAX_HEADER_VALUE_LENGTH = 1024
RESERVED_DLQ_ERROR_HEADERS = frozenset(
    {
        "error.stage",
        "error.type",
        "error.message",
        "error.http_status",
        "error.response_body",
    }
)


@dataclass(frozen=True)
class DlqErrorContext:
    """DLQ header metadata that explains why a message was routed to DLQ."""

    stage: str
    error_type: str
    message: str
    http_status: int | None = None
    response_body: str | None = None
    extra_headers: dict[str, str] | None = None


def _truncate(value: str, *, limit: int = MAX_HEADER_VALUE_LENGTH) -> str:
    """Truncate a decoded string value to the configured header size limit."""
    return value[:limit]


def enrich_dlq_headers(
    headers: Mapping[str, str] | None,
    error: DlqErrorContext,
) -> dict[str, str]:
    """Return a DLQ-enriched copy of normalized input headers."""
    enriched = dict(headers or {})
    enriched["error.stage"] = _truncate(error.stage)
    enriched["error.type"] = _truncate(error.error_type)
    enriched["error.message"] = _truncate(error.message)

    if error.http_status is not None:
        enriched["error.http_status"] = _truncate(str(error.http_status))
    if error.response_body is not None:
        enriched["error.response_body"] = _truncate(error.response_body)
    if error.extra_headers:
        for key, value in error.extra_headers.items():
            if key in RESERVED_DLQ_ERROR_HEADERS:
                continue
            enriched[key] = _truncate(value)

    return enriched
