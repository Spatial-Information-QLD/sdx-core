from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import cast

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type

from sdx_core.errors import TransientError
from sdx_core.esri.constants import (
    APPLY_EDITS_STOP_MESSAGE,
    AUTH_ERROR_CODES,
    DEPENDENCY_MISCONFIG_STATUSES,
    RETRY_STATUSES,
    TOKEN_STOP_MESSAGE,
)
from sdx_core.esri.helpers import (
    count_rejected_edits,
    extract_esri_error_code,
    summarize_esri_error,
)
from sdx_core.esri.url import derive_token_url
from sdx_core.retry import (
    RetryBackoffPolicy,
    build_exponential_jitter_retrying,
    build_interruptible_sleep,
)


class EsriRequestError(RuntimeError):
    """Base exception for ESRI request failures."""

    def __init__(
        self,
        message: str,
        http_status: int | None = None,
        response_body: str | None = None,
    ) -> None:
        """Initialize request-error metadata.

        Args:
            message: Human-readable error message.
            http_status: Optional HTTP status observed from ESRI.
            response_body: Optional response payload text.
        """
        super().__init__(message)
        self.http_status = http_status
        self.response_body = response_body


class EsriAuthUnavailable(RuntimeError):
    """Raised when ESRI auth/token dependency is unavailable."""


class EsriProcessingInterrupted(EsriAuthUnavailable):
    """Raised when shutdown interrupts token or applyEdits processing."""


class EsriTransientFailure(EsriRequestError, TransientError):
    """Raised for retryable ESRI failures."""


class EsriDependencyMisconfigured(RuntimeError):
    """Raised when ESRI endpoint appears misconfigured."""

    def __init__(
        self,
        message: str,
        http_status: int | None = None,
        response_body: str | None = None,
    ) -> None:
        """Initialize misconfiguration error metadata.

        Args:
            message: Human-readable error message.
            http_status: Optional HTTP status observed from ESRI.
            response_body: Optional response payload text.
        """
        super().__init__(message)
        self.http_status = http_status
        self.response_body = response_body


class EsriInvalidToken(EsriRequestError):
    """Raised when ESRI indicates the token is invalid."""


class EsriRejectedPayload(EsriRequestError):
    """Raised when ESRI rejects all or part of the applyEdits payload."""

    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        response_body: str | None = None,
        rejected_count: int | None = None,
        total_count: int | None = None,
        esri_code: int | None = None,
    ) -> None:
        """Initialize rejected-payload metadata.

        Args:
            message: Human-readable rejection summary.
            http_status: Optional HTTP status observed from ESRI.
            response_body: Optional response payload text.
            rejected_count: Number of rejected edit items when known.
            total_count: Total edit count in the request when known.
            esri_code: Optional ESRI error code extracted from the response.
        """
        super().__init__(message, http_status=http_status, response_body=response_body)
        self.rejected_count = rejected_count
        self.total_count = total_count
        self.esri_code = esri_code


@dataclass(frozen=True)
class _Token:
    """Cached token value with absolute expiry epoch."""

    value: str
    expires_at_epoch: float

    def is_expired(self, buffer_seconds: float) -> bool:
        """Return true when token expiry is within the safety window."""
        return time.time() >= (self.expires_at_epoch - buffer_seconds)


class FeatureServiceClient:
    """Feature service client with bounded retries and token recovery."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        feature_service_url: str,
        username: str,
        password: str,
        referer: str,
        token_expiration_minutes: int,
        token_expiration_buffer_seconds: float,
        token_retry_attempts: int = 4,
        apply_retry_attempts: int = 4,
        retry_min_seconds: float = 1.0,
        retry_max_seconds: float = 3.0,
    ) -> None:
        """Create a feature-service client with token and applyEdits retries.

        Args:
            client: Shared async HTTP client.
            feature_service_url: ESRI feature service layer URL.
            username: Username used for token generation.
            password: Password used for token generation.
            referer: Referer header required by ESRI token endpoint.
            token_expiration_minutes: Requested token lifetime in minutes.
            token_expiration_buffer_seconds: Safety buffer before token expiry.
            token_retry_attempts: Max token request attempts.
            apply_retry_attempts: Max applyEdits attempts.
            retry_min_seconds: Minimum retry backoff in seconds.
            retry_max_seconds: Maximum retry backoff in seconds.
        """
        self._client = client
        self._base_url = feature_service_url.rstrip("/")
        self._token_url = derive_token_url(feature_service_url)
        self._username = username
        self._password = password
        self._referer = referer
        self._token_expiration_minutes = token_expiration_minutes
        self._token_expiration_buffer_seconds = token_expiration_buffer_seconds
        self._token_retry_policy = RetryBackoffPolicy(
            attempts=token_retry_attempts,
            min_seconds=retry_min_seconds,
            max_seconds=retry_max_seconds,
        )
        self._apply_retry_policy = RetryBackoffPolicy(
            attempts=apply_retry_attempts,
            min_seconds=retry_min_seconds,
            max_seconds=retry_max_seconds,
        )
        self._token_lock = asyncio.Lock()
        self._token: _Token | None = None
        self._auth_failed = False

    def is_available(self) -> bool:
        """Return dependency availability state for readiness probes."""
        return self._token is not None and not self._auth_failed

    async def ensure_token(self, *, stop_event: asyncio.Event | None = None) -> str:
        """Ensure a valid token is available and return it."""
        self._raise_if_stopped(stop_event, TOKEN_STOP_MESSAGE)
        cached_token = self._cached_token_value()
        if cached_token is not None:
            return cached_token

        async with self._token_lock:
            self._raise_if_stopped(stop_event, TOKEN_STOP_MESSAGE)
            cached_token = self._cached_token_value()
            if cached_token is not None:
                return cached_token
            return await self._refresh_token_with_retry(stop_event=stop_event)

    async def invalidate_token(self) -> None:
        """Clear the cached token so next request forces refresh."""
        async with self._token_lock:
            self._token = None

    async def apply_edits(
        self,
        payload: dict[str, object],
        *,
        stop_event: asyncio.Event | None = None,
    ) -> dict[str, object]:
        """Submit one applyEdits request with transient retry and token recovery."""
        self._raise_if_stopped(stop_event, APPLY_EDITS_STOP_MESSAGE)
        retrying = self._build_retrying(
            retry_for=(EsriTransientFailure, EsriInvalidToken),
            policy=self._apply_retry_policy,
            stop_event=stop_event,
        )

        try:
            async for attempt in retrying:
                with attempt:
                    self._raise_if_stopped(stop_event, APPLY_EDITS_STOP_MESSAGE)
                    response_data = await self._apply_edits_once(
                        payload,
                        stop_event=stop_event,
                    )
                    self._auth_failed = False
                    return response_data
        except EsriInvalidToken as exc:
            self._auth_failed = True
            raise EsriAuthUnavailable(
                "Token recovery failed after retry exhaustion."
            ) from exc
        except EsriProcessingInterrupted:
            raise
        except EsriAuthUnavailable:
            self._auth_failed = True
            raise

        raise RuntimeError("applyEdits retry loop exited unexpectedly.")

    def _cached_token_value(self) -> str | None:
        token = self._token
        if token is None:
            return None
        if token.is_expired(self._token_expiration_buffer_seconds):
            return None
        return token.value

    async def _refresh_token_with_retry(
        self,
        *,
        stop_event: asyncio.Event | None,
    ) -> str:
        retrying = self._build_retrying(
            retry_for=(EsriTransientFailure,),
            policy=self._token_retry_policy,
            stop_event=stop_event,
        )
        try:
            async for attempt in retrying:
                with attempt:
                    self._raise_if_stopped(stop_event, TOKEN_STOP_MESSAGE)
                    token = await self._request_token_once()
                    self._token = token
                    self._auth_failed = False
                    return token.value
        except EsriTransientFailure:
            self._auth_failed = True
            raise
        except EsriProcessingInterrupted:
            raise
        except EsriAuthUnavailable:
            self._auth_failed = True
            raise

        raise RuntimeError("Token retry loop exited unexpectedly.")

    async def _request_token_once(self) -> _Token:
        form_data = {
            "f": "json",
            "username": self._username,
            "password": self._password,
            "referer": self._referer,
            "expiration": str(self._token_expiration_minutes),
        }

        try:
            response = await self._client.post(self._token_url, data=form_data)
        except httpx.RequestError as exc:
            raise EsriTransientFailure(str(exc)) from exc

        if response.status_code in RETRY_STATUSES:
            raise EsriTransientFailure(
                f"Token endpoint transient failure (HTTP {response.status_code}).",
                http_status=response.status_code,
                response_body=response.text,
            )
        if response.status_code in {401, 403}:
            raise EsriAuthUnavailable(
                f"Token endpoint auth failed (HTTP {response.status_code})."
            )
        if response.status_code >= 400:
            raise EsriAuthUnavailable(
                f"Token endpoint returned HTTP {response.status_code}."
            )

        payload = self._parse_json_object(
            response,
            context_message="Token response is not a valid JSON object.",
        )

        if "error" in payload:
            summary = summarize_esri_error(payload["error"])
            raise EsriAuthUnavailable(f"Token request failed: {summary}")

        token = payload.get("token")
        if not isinstance(token, str) or not token:
            raise EsriAuthUnavailable("Token response missing token field.")

        expires_at_epoch = self._extract_expiry(payload)
        return _Token(value=token, expires_at_epoch=expires_at_epoch)

    async def _apply_edits_once(
        self,
        payload: dict[str, object],
        *,
        stop_event: asyncio.Event | None,
    ) -> dict[str, object]:
        token = await self.ensure_token(stop_event=stop_event)
        url = f"{self._base_url}/applyEdits"
        form_data = self._build_form_data(token, payload)

        try:
            response = await self._client.post(url, data=form_data)
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            self._raise_for_http_status(exc)
        except httpx.RequestError as exc:
            raise EsriTransientFailure(str(exc)) from exc

        response_data = self._parse_response_data(response)
        try:
            self._raise_for_esri_error(
                response_data=response_data,
                http_status=response.status_code,
            )
            self._raise_for_rejected_edits(
                response_data=response_data,
                http_status=response.status_code,
            )
        except EsriInvalidToken:
            # Force immediate token refresh before retrying this applyEdits call.
            await self.invalidate_token()
            await self.ensure_token(stop_event=stop_event)
            raise

        return response_data

    @staticmethod
    def _build_form_data(token: str, payload: dict[str, object]) -> dict[str, str]:
        form_data: dict[str, str] = {
            "f": "json",
            "token": token,
            "rollbackOnFailure": "true",
        }
        for key, value in payload.items():
            form_data[key] = json.dumps(value)
        return form_data

    @staticmethod
    def _parse_json_object(
        response: httpx.Response,
        *,
        context_message: str,
    ) -> dict[str, object]:
        try:
            response_data = response.json()
        except ValueError as exc:
            raise EsriRequestError(
                context_message,
                http_status=response.status_code,
                response_body=response.text,
            ) from exc

        if not isinstance(response_data, dict):
            raise EsriRequestError(
                context_message,
                http_status=response.status_code,
                response_body=response.text,
            )
        return cast(dict[str, object], response_data)

    @classmethod
    def _parse_response_data(cls, response: httpx.Response) -> dict[str, object]:
        return cls._parse_json_object(
            response,
            context_message="applyEdits response is not a valid JSON object.",
        )

    @staticmethod
    def _raise_for_http_status(exc: httpx.HTTPStatusError) -> None:
        status = exc.response.status_code
        response_body = exc.response.text
        message = str(exc)
        if status in RETRY_STATUSES:
            raise EsriTransientFailure(
                message,
                http_status=status,
                response_body=response_body,
            ) from exc
        if status in DEPENDENCY_MISCONFIG_STATUSES:
            raise EsriDependencyMisconfigured(
                message,
                http_status=status,
                response_body=response_body,
            ) from exc
        if status in {401, 403}:
            raise EsriAuthUnavailable(
                f"ESRI applyEdits auth failed (HTTP {status})."
            ) from exc
        raise EsriRejectedPayload(
            message,
            http_status=status,
            response_body=response_body,
        ) from exc

    @staticmethod
    def _raise_for_esri_error(
        *,
        response_data: dict[str, object],
        http_status: int,
    ) -> None:
        if "error" not in response_data:
            return

        error = response_data["error"]
        esri_code = extract_esri_error_code(error)
        response_body = json.dumps(response_data)

        if esri_code in AUTH_ERROR_CODES:
            raise EsriInvalidToken(
                summarize_esri_error(error),
                http_status=http_status,
                response_body=response_body,
            )
        if esri_code in RETRY_STATUSES:
            raise EsriTransientFailure(
                summarize_esri_error(error),
                http_status=http_status,
                response_body=response_body,
            )
        raise EsriRejectedPayload(
            summarize_esri_error(error),
            http_status=http_status,
            response_body=response_body,
            esri_code=esri_code,
        )

    @staticmethod
    def _raise_for_rejected_edits(
        *,
        response_data: dict[str, object],
        http_status: int,
    ) -> None:
        rejected_count, total_count = count_rejected_edits(response_data)
        if not rejected_count:
            return
        raise EsriRejectedPayload(
            f"ESRI applyEdits rejected {rejected_count}/{total_count} edits.",
            http_status=http_status,
            response_body=json.dumps(response_data),
            rejected_count=rejected_count,
            total_count=total_count,
        )

    @staticmethod
    def _extract_expiry(payload: dict[str, object]) -> float:
        expires = payload.get("expires")
        if isinstance(expires, (int, float)):
            return float(expires) / 1000.0
        return time.time() + (55 * 60)

    @staticmethod
    def _raise_if_stopped(stop_event: asyncio.Event | None, message: str) -> None:
        if stop_event is not None and stop_event.is_set():
            raise EsriProcessingInterrupted(message)

    @staticmethod
    def _build_retrying(
        *,
        retry_for: tuple[type[BaseException], ...],
        policy: RetryBackoffPolicy,
        stop_event: asyncio.Event | None,
    ) -> AsyncRetrying:
        sleep = None
        if stop_event is not None:
            sleep = build_interruptible_sleep(stop_event)
        return build_exponential_jitter_retrying(
            retry=retry_if_exception_type(retry_for),
            policy=policy,
            sleep=sleep,
            reraise=True,
        )
