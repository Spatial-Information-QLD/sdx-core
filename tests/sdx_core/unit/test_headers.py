from __future__ import annotations

from sdx_core.headers import (
    MAX_HEADER_VALUE_LENGTH,
    DlqErrorContext,
    _truncate,
    enrich_dlq_headers,
)


def test_truncate_applies_default_limit() -> None:
    value = "x" * (MAX_HEADER_VALUE_LENGTH + 10)
    assert _truncate(value) == "x" * MAX_HEADER_VALUE_LENGTH


def test_enrich_dlq_headers_merges_optional_fields_and_skips_reserved_extra() -> None:
    long_value = "z" * (MAX_HEADER_VALUE_LENGTH + 5)
    context = DlqErrorContext(
        stage=long_value,
        error_type="ValidationError",
        message=long_value,
        http_status=503,
        response_body=long_value,
        extra_headers={
            "x-extra": long_value,
            "error.type": "must-not-overwrite",
        },
    )

    enriched = enrich_dlq_headers({"existing": "value"}, context)

    assert enriched["existing"] == "value"
    assert enriched["error.stage"] == "z" * MAX_HEADER_VALUE_LENGTH
    assert enriched["error.type"] == "ValidationError"
    assert enriched["error.message"] == "z" * MAX_HEADER_VALUE_LENGTH
    assert enriched["error.http_status"] == "503"
    assert enriched["error.response_body"] == "z" * MAX_HEADER_VALUE_LENGTH
    assert enriched["x-extra"] == "z" * MAX_HEADER_VALUE_LENGTH
    assert enriched["error.type"] != "must-not-overwrite"


def test_enrich_dlq_headers_handles_empty_inputs() -> None:
    context = DlqErrorContext(
        stage="parse",
        error_type="RuntimeError",
        message="boom",
    )

    enriched = enrich_dlq_headers(None, context)

    assert enriched == {
        "error.stage": "parse",
        "error.type": "RuntimeError",
        "error.message": "boom",
    }
