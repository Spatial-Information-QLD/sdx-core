from __future__ import annotations

from sdx_core.esri.helpers import (
    count_rejected_edits,
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


def test_count_rejected_edits_ignores_non_boolean_success_values() -> None:
    rejected, total = count_rejected_edits(
        {
            "addResults": [
                {"success": True},
                {"success": False},
                {"success": "false"},
                "not-a-dict",
            ],
            "updateResults": None,
            "deleteResults": [
                {"success": False},
            ],
        }
    )

    assert rejected == 2
    assert total == 3
