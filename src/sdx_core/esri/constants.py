"""Shared constants used by the sdx_core ESRI feature-service client."""

RETRY_STATUSES = {408, 429, 500, 502, 503, 504}
AUTH_ERROR_CODES = {498, 499}
DEPENDENCY_MISCONFIG_STATUSES = {404, 405}
TOKEN_STOP_MESSAGE = "Shutdown requested during token refresh."
APPLY_EDITS_STOP_MESSAGE = "Shutdown requested before applyEdits."
