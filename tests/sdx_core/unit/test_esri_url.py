from __future__ import annotations

import pytest

from sdx_core.esri.url import derive_token_url


@pytest.mark.parametrize(
    ("feature_service_url", "expected"),
    [
        (
            "https://example.com/arcgis/rest/services/Layer/FeatureServer/0",
            "https://example.com/arcgis/sharing/rest/generateToken",
        ),
        (
            "https://example.com/arcgis/rest",
            "https://example.com/arcgis/sharing/rest/generateToken",
        ),
        (
            "https://example.com/custom/path",
            "https://example.com/custom/path/sharing/rest/generateToken",
        ),
        (
            "https://example.com",
            "https://example.com/sharing/rest/generateToken",
        ),
    ],
)
def test_derive_token_url(feature_service_url: str, expected: str) -> None:
    assert derive_token_url(feature_service_url) == expected
