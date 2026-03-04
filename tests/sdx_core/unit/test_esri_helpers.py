from __future__ import annotations

from sdx_core.esri.helpers import (
    extract_esri_error_code,
    summarize_esri_error,
)


def test_summarize_esri_error_formats_code_and_message() -> None:
    assert summarize_esri_error({"code": 498, "message": "Invalid token"}) == (
        "ESRI error 498: Invalid token"
    )


def test_summarize_esri_error_falls_back_to_json_dump() -> None:
    assert summarize_esri_error({"code": "498", "message": "Invalid token"}) == (
        '{"code": "498", "message": "Invalid token"}'
    )


def test_extract_esri_error_code_requires_integer_code() -> None:
    assert extract_esri_error_code({"code": 503}) == 503
    assert extract_esri_error_code({"code": "503"}) is None
    assert extract_esri_error_code("not-a-dict") is None
