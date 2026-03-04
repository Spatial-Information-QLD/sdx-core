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
