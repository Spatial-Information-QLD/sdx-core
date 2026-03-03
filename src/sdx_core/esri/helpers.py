"""Helpers for parsing and summarizing ESRI applyEdits/token responses."""

from __future__ import annotations

import json
from typing import cast


def summarize_esri_error(error: object) -> str:
    """Create a concise ESRI error summary string."""
    if isinstance(error, dict):
        code = error.get("code")
        message = error.get("message")
        if isinstance(code, int) and isinstance(message, str) and message:
            return f"ESRI error {code}: {message}"
    return json.dumps(error)


def extract_esri_error_code(error: object) -> int | None:
    """Extract integer ESRI error code from a response error object."""
    if isinstance(error, dict) and isinstance(error.get("code"), int):
        return cast(int, error["code"])
    return None


def count_rejected_layer_edits(response_data: dict[str, object]) -> tuple[int, int]:
    """Count failed edit items from a layer-level applyEdits response payload."""
    return _count_rejected_mapping_edits(response_data)


def count_rejected_service_edits(response_data: dict[str, object]) -> tuple[int, int]:
    """Count failed edit items from a service-level applyEdits response payload."""
    rejected = 0
    total = 0
    edited_feature_results = response_data.get("editedFeatureResults")
    if not isinstance(edited_feature_results, list):
        return rejected, total

    for item in edited_feature_results:
        if not isinstance(item, dict):
            continue
        item_rejected, item_total = _count_rejected_mapping_edits(
            cast(dict[str, object], item)
        )
        rejected += item_rejected
        total += item_total

    return rejected, total


def _count_rejected_mapping_edits(response_data: dict[str, object]) -> tuple[int, int]:
    rejected = 0
    total = 0
    for key in ("addResults", "updateResults", "deleteResults"):
        results = response_data.get(key)
        if not isinstance(results, list):
            continue
        for item in results:
            if not isinstance(item, dict):
                continue
            success = item.get("success")
            if not isinstance(success, bool):
                continue
            total += 1
            if not success:
                rejected += 1
    return rejected, total
