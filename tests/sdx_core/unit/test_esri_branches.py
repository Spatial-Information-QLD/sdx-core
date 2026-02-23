from __future__ import annotations

import asyncio
import time

import httpx
import pytest
from pytest_httpx import HTTPXMock

import sdx_core.esri as esri_mod
from sdx_core.esri import (
    EsriAuthUnavailable,
    EsriProcessingInterrupted,
    EsriRejectedPayload,
    EsriRequestError,
    EsriTransientFailure,
    FeatureServiceClient,
)
from sdx_core.retry import RetryBackoffPolicy

pytestmark = pytest.mark.asyncio

_FEATURE_SERVICE_URL = "https://example.com/arcgis/rest/services/Layer/FeatureServer/0"
_TOKEN_URL = "https://example.com/arcgis/sharing/rest/generateToken"


def _build_client(
    http_client: httpx.AsyncClient,
    *,
    token_retry_attempts: int = 1,
    apply_retry_attempts: int = 1,
) -> FeatureServiceClient:
    return FeatureServiceClient(
        client=http_client,
        feature_service_url=_FEATURE_SERVICE_URL,
        username="user",
        password="pass",
        referer="https://app.local",
        token_expiration_minutes=60,
        token_expiration_buffer_seconds=60.0,
        token_retry_attempts=token_retry_attempts,
        apply_retry_attempts=apply_retry_attempts,
        retry_min_seconds=0.0,
        retry_max_seconds=0.0,
    )


class _NoAttemptRetrying:
    def __aiter__(self) -> _NoAttemptRetrying:
        return self

    async def __anext__(self) -> object:
        raise StopAsyncIteration


async def test_ensure_token_rechecks_cache_inside_token_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)

        async def _never_refresh(*, stop_event: asyncio.Event | None) -> str:
            _ = stop_event
            raise AssertionError("refresh should not be called")

        monkeypatch.setattr(client, "_refresh_token_with_retry", _never_refresh)
        await client._token_lock.acquire()
        token_task = asyncio.create_task(client.ensure_token())
        await asyncio.sleep(0)

        client._token = esri_mod._Token("cached", time.time() + 600.0)
        client._token_lock.release()

        assert await token_task == "cached"


async def test_apply_edits_maps_invalid_token_exhaustion_to_auth_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)

        async def _raise_invalid(
            payload: dict[str, object],
            *,
            stop_event: asyncio.Event | None,
        ) -> dict[str, object]:
            _ = (payload, stop_event)
            raise esri_mod.EsriInvalidToken("bad token")

        monkeypatch.setattr(client, "_apply_edits_once", _raise_invalid)

        with pytest.raises(EsriAuthUnavailable, match="Token recovery failed"):
            await client.apply_edits({})
        assert client._auth_failed is True


async def test_apply_edits_propagates_processing_interrupted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)

        async def _raise_interrupted(
            payload: dict[str, object],
            *,
            stop_event: asyncio.Event | None,
        ) -> dict[str, object]:
            _ = (payload, stop_event)
            raise EsriProcessingInterrupted("stop now")

        monkeypatch.setattr(client, "_apply_edits_once", _raise_interrupted)

        with pytest.raises(EsriProcessingInterrupted):
            await client.apply_edits({})


async def test_apply_edits_sets_auth_failed_when_auth_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)

        async def _raise_auth_unavailable(
            payload: dict[str, object],
            *,
            stop_event: asyncio.Event | None,
        ) -> dict[str, object]:
            _ = (payload, stop_event)
            raise EsriAuthUnavailable("auth dependency unavailable")

        monkeypatch.setattr(client, "_apply_edits_once", _raise_auth_unavailable)

        with pytest.raises(EsriAuthUnavailable):
            await client.apply_edits({})
        assert client._auth_failed is True


async def test_apply_edits_raises_runtime_error_if_retry_loop_returns_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        monkeypatch.setattr(
            client, "_build_retrying", lambda **kwargs: _NoAttemptRetrying()
        )

        with pytest.raises(
            RuntimeError, match="applyEdits retry loop exited unexpectedly"
        ):
            await client.apply_edits({})


async def test_refresh_token_with_retry_propagates_processing_interrupted() -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        stop_event = asyncio.Event()
        stop_event.set()

        with pytest.raises(EsriProcessingInterrupted):
            await client._refresh_token_with_retry(stop_event=stop_event)


async def test_refresh_token_with_retry_raises_runtime_error_if_retry_loop_exits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        monkeypatch.setattr(
            client, "_build_retrying", lambda **kwargs: _NoAttemptRetrying()
        )

        with pytest.raises(RuntimeError, match="Token retry loop exited unexpectedly"):
            await client._refresh_token_with_retry(stop_event=None)


async def test_request_token_once_wraps_request_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)

        async def _raise_request_error(
            url: str, data: dict[str, str]
        ) -> httpx.Response:
            _ = data
            raise httpx.ConnectError(
                "connect failed", request=httpx.Request("POST", url)
            )

        monkeypatch.setattr(http_client, "post", _raise_request_error)

        with pytest.raises(EsriTransientFailure, match="connect failed"):
            await client._request_token_once()


async def test_request_token_once_maps_non_auth_http_error_to_auth_unavailable(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST", url=_TOKEN_URL, status_code=418, text="teapot"
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        with pytest.raises(EsriAuthUnavailable, match="returned HTTP 418"):
            await client._request_token_once()


async def test_request_token_once_maps_error_payload_to_auth_unavailable(
    httpx_mock: HTTPXMock,
) -> None:
    httpx_mock.add_response(
        method="POST",
        url=_TOKEN_URL,
        json={"error": {"code": 1, "message": "invalid credentials"}},
    )

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        with pytest.raises(EsriAuthUnavailable, match="Token request failed"):
            await client._request_token_once()


async def test_request_token_once_requires_token_field(httpx_mock: HTTPXMock) -> None:
    httpx_mock.add_response(method="POST", url=_TOKEN_URL, json={"expires": 123})

    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        with pytest.raises(EsriAuthUnavailable, match="missing token field"):
            await client._request_token_once()


async def test_apply_edits_once_wraps_request_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with httpx.AsyncClient() as http_client:
        client = _build_client(http_client)
        client._token = esri_mod._Token("abc", time.time() + 3600.0)

        async def _raise_request_error(
            url: str, data: dict[str, str]
        ) -> httpx.Response:
            _ = data
            raise httpx.ConnectError(
                "downstream unavailable", request=httpx.Request("POST", url)
            )

        monkeypatch.setattr(http_client, "post", _raise_request_error)

        with pytest.raises(EsriTransientFailure, match="downstream unavailable"):
            await client._apply_edits_once({}, stop_event=None)


async def test_parse_json_object_raises_for_invalid_json_payload() -> None:
    response = httpx.Response(status_code=200, text="not-json")

    with pytest.raises(EsriRequestError, match="invalid JSON object"):
        FeatureServiceClient._parse_json_object(
            response,
            context_message="invalid JSON object",
        )


async def test_parse_json_object_raises_for_non_mapping_json_payload() -> None:
    response = httpx.Response(status_code=200, json=["not", "an", "object"])

    with pytest.raises(EsriRequestError, match="must be object"):
        FeatureServiceClient._parse_json_object(
            response,
            context_message="must be object",
        )


async def test_raise_for_http_status_maps_auth_statuses() -> None:
    request = httpx.Request("POST", f"{_FEATURE_SERVICE_URL}/applyEdits")
    response = httpx.Response(status_code=403, text="forbidden", request=request)
    error = httpx.HTTPStatusError("forbidden", request=request, response=response)

    with pytest.raises(EsriAuthUnavailable, match="auth failed"):
        FeatureServiceClient._raise_for_http_status(error)


async def test_raise_for_http_status_maps_non_special_status_to_rejected_payload() -> (
    None
):
    request = httpx.Request("POST", f"{_FEATURE_SERVICE_URL}/applyEdits")
    response = httpx.Response(status_code=400, text="bad request", request=request)
    error = httpx.HTTPStatusError("bad request", request=request, response=response)

    with pytest.raises(EsriRejectedPayload):
        FeatureServiceClient._raise_for_http_status(error)


async def test_raise_for_esri_error_maps_retry_codes_to_transient_failure() -> None:
    with pytest.raises(EsriTransientFailure, match="ESRI error 503"):
        FeatureServiceClient._raise_for_esri_error(
            response_data={"error": {"code": 503, "message": "unavailable"}},
            http_status=200,
        )


async def test_raise_for_esri_error_maps_other_codes_to_rejected_payload() -> None:
    with pytest.raises(EsriRejectedPayload):
        FeatureServiceClient._raise_for_esri_error(
            response_data={"error": {"code": 400, "message": "invalid payload"}},
            http_status=200,
        )


async def test_extract_expiry_falls_back_when_expires_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(time, "time", lambda: 1000.0)
    assert FeatureServiceClient._extract_expiry({}) == 4300.0


async def test_build_retrying_builds_interruptible_sleep_when_stop_event_present() -> (
    None
):
    retrying = FeatureServiceClient._build_retrying(
        retry_for=(EsriTransientFailure,),
        policy=RetryBackoffPolicy(attempts=1, min_seconds=0.0, max_seconds=0.0),
        stop_event=asyncio.Event(),
    )

    assert retrying is not None
