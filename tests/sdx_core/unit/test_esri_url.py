from __future__ import annotations

import pytest

from sdx_core.esri.url import (
    build_layer_apply_edits_url,
    build_service_apply_edits_url,
    derive_token_url,
    is_feature_service_layer_url,
)


@pytest.mark.parametrize(
    ("feature_service_url", "expected"),
    [
        (
            "https://example.com/arcgis/rest/services/Layer/FeatureServer",
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


@pytest.mark.parametrize(
    ("feature_service_url", "expected"),
    [
        ("https://example.com", False),
        ("https://example.com/arcgis/rest/services/Layer/FeatureServer", False),
        ("https://example.com/arcgis/rest/services/Layer/FeatureServer/0", True),
        ("https://example.com/arcgis/rest/services/Layer/MapServer/0", False),
    ],
)
def test_is_feature_service_layer_url(feature_service_url: str, expected: bool) -> None:
    assert is_feature_service_layer_url(feature_service_url) is expected


def test_build_service_apply_edits_url() -> None:
    assert build_service_apply_edits_url(
        "https://example.com/arcgis/rest/services/Layer/FeatureServer"
    ) == ("https://example.com/arcgis/rest/services/Layer/FeatureServer/applyEdits")


def test_build_layer_apply_edits_url() -> None:
    assert build_layer_apply_edits_url(
        "https://example.com/arcgis/rest/services/Layer/FeatureServer",
        3,
    ) == ("https://example.com/arcgis/rest/services/Layer/FeatureServer/3/applyEdits")
