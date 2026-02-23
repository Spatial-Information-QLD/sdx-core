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
