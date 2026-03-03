"""URL helpers for ESRI feature service integration."""

from urllib.parse import urlsplit


def derive_token_url(feature_service_url: str) -> str:
    """Derive the ArcGIS token URL from a feature service URL."""
    parsed = urlsplit(feature_service_url)
    path = parsed.path or ""

    if "/rest/" in path:
        prefix = path.split("/rest/", 1)[0].rstrip("/")
    elif path.endswith("/rest"):
        prefix = path[: -len("/rest")].rstrip("/")
    else:
        prefix = path.rstrip("/")

    token_path = "/sharing/rest/generateToken"
    if prefix:
        token_path = f"{prefix}{token_path}"

    return f"{parsed.scheme}://{parsed.netloc}{token_path}"


def is_feature_service_layer_url(feature_service_url: str) -> bool:
    """Return true when the URL targets a specific FeatureServer layer."""
    path = (urlsplit(feature_service_url).path or "").rstrip("/")
    parts = [part for part in path.split("/") if part]
    if len(parts) < 2:
        return False
    if parts[-2] != "FeatureServer":
        return False
    return parts[-1].isdigit()


def build_service_apply_edits_url(feature_service_url: str) -> str:
    """Build the applyEdits URL for a FeatureServer service endpoint."""
    return f"{feature_service_url.rstrip('/')}/applyEdits"


def build_layer_apply_edits_url(feature_service_url: str, layer_id: int) -> str:
    """Build the applyEdits URL for a specific FeatureServer layer endpoint."""
    return f"{feature_service_url.rstrip('/')}/{layer_id}/applyEdits"
