from __future__ import annotations

import asyncio
import time
from typing import cast
from urllib.parse import parse_qs

import httpx
import pytest
from pytest_httpx import HTTPXMock

from sdx_core.errors import TransientError
from sdx_core.esri import (
    EsriAuthUnavailable,
    EsriDependencyMisconfigured,
    EsriProcessingInterrupted,
    EsriRejectedPayload,
    EsriRequestError,
    EsriTransientFailure,
    FeatureServiceClient,
)

pytestmark = pytest.mark.asyncio

_FEATURE_SERVICE_URL = "https://example.com/arcgis/rest/services/Layer/FeatureServer"
_TOKEN_URL = "https://example.com/arcgis/sharing/rest/generateToken"
_SERVICE_APPLY_EDITS_URL = f"{_FEATURE_SERVICE_URL}/applyEdits"
_LAYER_APPLY_EDITS_URL = f"{_FEATURE_SERVICE_URL}/0/applyEdits"


def _build_client(
    http_client: httpx.AsyncClient,
    *,
    token_retry_attempts: int | None = None,
    apply_retry_attempts: int | None = None,
    retry_min_seconds: float | None = None,
    retry_max_seconds: float | None = None,
) -> FeatureServiceClient:
    resolved_token_retry_attempts = (
        4 if token_retry_attempts is None else token_retry_attempts
    )
    resolved_apply_retry_attempts = (
        4 if apply_retry_attempts is None else apply_retry_attempts
    )
    resolved_retry_min_seconds = 1.0 if retry_min_seconds is None else retry_min_seconds
    resolved_retry_max_seconds = 3.0 if retry_max_seconds is None else retry_max_seconds
    return FeatureServiceClient(
        client=http_client,
        feature_service_url=_FEATURE_SERVICE_URL,
        username="user",
        password="pass",
        referer="https://app.local",
        token_expiration_minutes=60,
        token_expiration_buffer_seconds=60.0,
        token_retry_attempts=resolved_token_retry_attempts,
        apply_retry_attempts=resolved_apply_retry_attempts,
        retry_min_seconds=resolved_retry_min_seconds,
        retry_max_seconds=resolved_retry_max_seconds,
    )


def _requests_for(httpx_mock: HTTPXMock, url: str) -> list[httpx.Request]:
    return [request for request in httpx_mock.get_requests() if str(request.url) == url]


async def test_ensure_token_caches_token(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        assert await client.ensure_token() == "abc"
        assert await client.ensure_token() == "abc"

    assert len(_requests_for(httpx_mock, _TOKEN_URL)) == 1


async def test_ensure_token_refreshes_when_cached_token_is_expired(
    httpx_mock: HTTPXMock,
) -> None:
    now_ms = int(time.time() * 1000)
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": now_ms + 1_000},
    )
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "def", "expires": now_ms + 120_000},
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        assert await client.ensure_token() == "abc"
        assert await client.ensure_token() == "def"

    assert len(_requests_for(httpx_mock, _TOKEN_URL)) == 2


async def test_ensure_token_is_concurrency_safe(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        tokens = await asyncio.gather(*[client.ensure_token() for _ in range(10)])

    assert tokens == ["abc"] * 10
    assert len(_requests_for(httpx_mock, _TOKEN_URL)) == 1


async def test_is_available_transitions_with_token_transient_failures(
    httpx_mock: HTTPXMock,
) -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client, token_retry_attempts=1)
        assert client.is_available() is False

        httpx_mock.add_response(
            method="POST",
            url=_TOKEN_URL,
            json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
        )
        assert await client.ensure_token() == "abc"
        assert client.is_available() is True

        await client.invalidate_token()
        httpx_mock.add_response(
            method="POST",
            url=_TOKEN_URL,
            status_code=500,
            text="boom",
        )
        with pytest.raises(EsriTransientFailure) as exc_info:
            await client.ensure_token()
        assert isinstance(exc_info.value, TransientError)
        assert isinstance(exc_info.value, EsriRequestError)
        assert client.is_available() is False

        httpx_mock.add_response(
            method="POST",
            url=_TOKEN_URL,
            json={"token": "def", "expires": int(time.time() * 1000) + 120_000},
        )
        assert await client.ensure_token() == "def"
        assert client.is_available() is True


async def test_ensure_token_maps_http_auth_error_to_auth_unavailable(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        status_code=401,
        text="unauthorized",
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client, token_retry_attempts=1)
        with pytest.raises(EsriAuthUnavailable):
            await client.ensure_token()


async def test_ensure_token_stop_event_raises_interrupted_without_auth_failure(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        assert await client.ensure_token() == "abc"
        assert client.is_available() is True

        stop_event = asyncio.Event()
        stop_event.set()
        with pytest.raises(EsriProcessingInterrupted):
            await client.ensure_token(stop_event=stop_event)
        assert client.is_available() is True

        stop_event.clear()
        assert await client.ensure_token(stop_event=stop_event) == "abc"
        assert client.is_available() is True

    assert len(_requests_for(httpx_mock, _TOKEN_URL)) == 1


async def test_apply_edits_to_layer_stop_event_raises_interrupted_without_auth_failure(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        assert await client.ensure_token() == "abc"
        assert client.is_available() is True

        stop_event = asyncio.Event()
        stop_event.set()
        with pytest.raises(EsriProcessingInterrupted):
            await client.apply_edits_to_layer(
                0,
                {"updates": [{"attributes": {"objectid": 1}}]},
                stop_event=stop_event,
            )
        assert client.is_available() is True

    assert len(_requests_for(httpx_mock, _TOKEN_URL)) == 1
    assert len(_requests_for(httpx_mock, _LAYER_APPLY_EDITS_URL)) == 0


async def test_apply_edits_to_layer_transient_retry_exhaustion_raises(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )
    for _ in range(4):
        httpx_mock.add_response(
            method="POST",
            url=_LAYER_APPLY_EDITS_URL,
            status_code=503,
            json={"error": {"code": 503, "message": "unavailable"}},
        )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        with pytest.raises(EsriTransientFailure) as exc_info:
            await client.apply_edits_to_layer(
                0, {"updates": [{"attributes": {"objectid": 1}}]}
            )
    assert isinstance(exc_info.value, TransientError)
    assert isinstance(exc_info.value, EsriRequestError)

    assert len(_requests_for(httpx_mock, _TOKEN_URL)) == 1
    assert len(_requests_for(httpx_mock, _LAYER_APPLY_EDITS_URL)) == 4


async def test_apply_edits_to_layer_invalid_token_recovers_and_succeeds(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )
    httpx_mock.add_response(
        method="POST",
        url=_LAYER_APPLY_EDITS_URL,
        json={"error": {"code": 498, "message": "invalid token"}},
    )
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "def", "expires": int(time.time() * 1000) + 120_000},
    )
    httpx_mock.add_response(
        method="POST",
        url=_LAYER_APPLY_EDITS_URL,
        json={"updateResults": [{"objectId": 1, "success": True}]},
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        response = await client.apply_edits_to_layer(
            0, {"updates": [{"attributes": {"objectid": 1}}]}
        )

    update_results = cast(list[dict[str, object]], response["updateResults"])
    assert update_results[0]["objectId"] == 1
    assert len(_requests_for(httpx_mock, _TOKEN_URL)) == 2
    assert len(_requests_for(httpx_mock, _LAYER_APPLY_EDITS_URL)) == 2

    apply_request = _requests_for(httpx_mock, _LAYER_APPLY_EDITS_URL)[0]
    apply_form = parse_qs(apply_request.content.decode("utf-8"))
    assert apply_form["f"] == ["json"]
    assert apply_form["rollbackOnFailure"] == ["true"]


async def test_apply_edits_to_service_returns_raw_array_and_preserves_form_values(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )
    httpx_mock.add_response(
        method="POST",
        url=_SERVICE_APPLY_EDITS_URL,
        text='[{"id":0,"editedFeatures":{"adds":[{"attributes":{"iri":"https://example.com/a/1","pid":123}}]}}]',
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        response = await client.apply_edits_to_service(
            {
                "edits": [{"id": 2}],
                "returnServiceEditsOption": "originalAndCurrentFeatures",
                "useGlobalIds": False,
                "layer": 3,
            }
        )

    assert response == [
        {
            "id": 0,
            "editedFeatures": {
                "adds": [
                    {
                        "attributes": {
                            "iri": "https://example.com/a/1",
                            "pid": 123,
                        }
                    }
                ]
            },
        }
    ]

    apply_request = _requests_for(httpx_mock, _SERVICE_APPLY_EDITS_URL)[0]
    apply_form = parse_qs(apply_request.content.decode("utf-8"))
    assert apply_form["edits"] == ['[{"id": 2}]']
    assert apply_form["rollbackOnFailure"] == ["true"]
    assert apply_form["returnServiceEditsOption"] == ["originalAndCurrentFeatures"]
    assert apply_form["useGlobalIds"] == ["false"]
    assert apply_form["layer"] == ["3"]


async def test_apply_edits_to_service_forces_transactional_form_values(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )
    httpx_mock.add_response(
        method="POST",
        url=_SERVICE_APPLY_EDITS_URL,
        text='[{"id":0,"editedFeatures":{"adds":[]}}]',
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        await client.apply_edits_to_service(
            {
                "edits": [{"id": 2}],
                "rollbackOnFailure": False,
                "returnServiceEditsOption": "none",
            }
        )

    apply_request = _requests_for(httpx_mock, _SERVICE_APPLY_EDITS_URL)[0]
    apply_form = parse_qs(apply_request.content.decode("utf-8"))
    assert apply_form["rollbackOnFailure"] == ["true"]
    assert apply_form["returnServiceEditsOption"] == ["originalAndCurrentFeatures"]


@pytest.mark.parametrize("status_code", [404, 405])
async def test_apply_edits_to_layer_maps_dependency_misconfigured_statuses(
    httpx_mock: HTTPXMock,
    status_code: int,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )
    httpx_mock.add_response(
        method="POST",
        url=_LAYER_APPLY_EDITS_URL,
        status_code=status_code,
        json={"error": {"code": status_code, "message": "misconfigured"}},
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        with pytest.raises(EsriDependencyMisconfigured):
            await client.apply_edits_to_layer(
                0, {"updates": [{"attributes": {"objectid": 1}}]}
            )


async def test_apply_edits_to_layer_forces_rollback_on_failure(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )
    httpx_mock.add_response(
        method="POST",
        url=_LAYER_APPLY_EDITS_URL,
        json={"updateResults": [{"objectId": 1, "success": True}]},
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        await client.apply_edits_to_layer(
            0,
            {
                "updates": [{"attributes": {"objectid": 1}}],
                "rollbackOnFailure": False,
            },
        )

    apply_request = _requests_for(httpx_mock, _LAYER_APPLY_EDITS_URL)[0]
    apply_form = parse_qs(apply_request.content.decode("utf-8"))
    assert apply_form["rollbackOnFailure"] == ["true"]


async def test_apply_edits_to_layer_maps_top_level_error_to_rejected_payload(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )
    httpx_mock.add_response(
        method="POST",
        url=_LAYER_APPLY_EDITS_URL,
        json={
            "error": {
                "code": 400,
                "extendedCode": -2147207418,
                "message": "Unable to complete operation.",
                "details": [
                    "Violated attribute constraint rule. [Error No: -1, ]",
                    "Operation rolled back.",
                ],
            }
        },
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        with pytest.raises(EsriRejectedPayload) as exc_info:
            await client.apply_edits_to_layer(
                0,
                {"updates": [{"attributes": {"objectid": 1}}]},
            )

    assert exc_info.value.esri_code == 400


async def test_apply_edits_to_service_maps_top_level_error_to_rejected_payload(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"token": "abc", "expires": int(time.time() * 1000) + 120_000},
    )
    httpx_mock.add_response(
        method="POST",
        url=_SERVICE_APPLY_EDITS_URL,
        json={
            "error": {
                "code": 400,
                "extendedCode": -2147207418,
                "message": "Unable to complete operation.",
                "details": [
                    "Violated attribute constraint rule. [Error No: -1, ]",
                    "Operation rolled back.",
                ],
            }
        },
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        with pytest.raises(EsriRejectedPayload) as exc_info:
            await client.apply_edits_to_service(
                {"edits": [{"id": 0, "adds": [{"attributes": {"objectid": 1}}]}]}
            )

    assert exc_info.value.esri_code == 400


async def test_apply_edits_to_layer_rejects_invalid_layer_id(
    httpx_mock: HTTPXMock,
) -> None:
    _ = httpx_mock

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        with pytest.raises(ValueError, match="layer_id must be >= 0"):
            await client.apply_edits_to_layer(
                -1,
                {"updates": [{"attributes": {"objectid": 1}}]},
            )
