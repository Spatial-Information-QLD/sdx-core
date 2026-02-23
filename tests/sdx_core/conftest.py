from __future__ import annotations

import pytest

from tests.sdx_core.support.runtime_fakes import (
    FakeAdminClient,
    FakeLogger,
    FakeRuntime,
)


@pytest.fixture
def fake_logger() -> FakeLogger:
    """Provide a fresh structured logger test double per test."""
    return FakeLogger()


@pytest.fixture
def fake_runtime() -> FakeRuntime:
    """Provide a fresh FastStream runtime test double per test."""
    return FakeRuntime()


@pytest.fixture
def fake_admin_client() -> FakeAdminClient:
    """Provide a fresh admin client test double per test."""
    return FakeAdminClient()
