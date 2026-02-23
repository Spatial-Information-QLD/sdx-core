from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any


class FakeLogger:
    """Capture structured logger events for assertions."""

    def __init__(self) -> None:
        self.events: list[str] = []
        self.calls: list[tuple[str, str, dict[str, object]]] = []

    def _record(self, level: str, event: str, **kwargs: object) -> None:
        self.calls.append((level, event, kwargs))

    def info(self, event: str, **kwargs: object) -> None:
        self.events.append(event)
        self._record("info", event, **kwargs)

    def warning(self, event: str, **kwargs: object) -> None:
        self._record("warning", event, **kwargs)

    def error(self, event: str, **kwargs: object) -> None:
        self._record("error", event, **kwargs)

    def exception(self, event: str, **kwargs: object) -> None:
        self._record("exception", event, **kwargs)


class FakeAsgiApp:
    """Minimal ASGI app wrapper that executes startup/shutdown hooks."""

    def __init__(self, app: Any, *, hook_sleep_seconds: float = 0.02) -> None:
        self._app = app
        self._hook_sleep_seconds = hook_sleep_seconds
        self.run_calls: list[dict[str, object]] = []

    async def run(
        self,
        *,
        log_level: int,
        run_extra_options: dict[str, object] | None = None,
    ) -> None:
        self.run_calls.append(
            {
                "log_level": log_level,
                "run_extra_options": run_extra_options,
            }
        )
        for hook in self._app.startup_hooks:
            await hook()
        await asyncio.sleep(self._hook_sleep_seconds)
        for hook in self._app.shutdown_hooks:
            await hook()


class FakeApp:
    """FastStream-like app surface used by service app tests."""

    def __init__(self, *, hook_sleep_seconds: float = 0.02) -> None:
        self.startup_hooks: list[Callable[[], Awaitable[None]]] = []
        self.shutdown_hooks: list[Callable[[], Awaitable[None]]] = []
        self.routes: tuple[tuple[str, object], ...] = ()
        self.asgi_app = FakeAsgiApp(self, hook_sleep_seconds=hook_sleep_seconds)

    def on_startup(
        self,
        func: Callable[[], Awaitable[None]],
    ) -> Callable[[], Awaitable[None]]:
        self.startup_hooks.append(func)
        return func

    def on_shutdown(
        self,
        func: Callable[[], Awaitable[None]],
    ) -> Callable[[], Awaitable[None]]:
        self.shutdown_hooks.append(func)
        return func

    def as_asgi(self, *, asgi_routes: tuple[tuple[str, object], ...]) -> FakeAsgiApp:
        self.routes = asgi_routes
        return self.asgi_app


class FakeRuntime:
    """Stream runtime holder with a fake FastStream app."""

    def __init__(self, *, hook_sleep_seconds: float = 0.02) -> None:
        self.app = FakeApp(hook_sleep_seconds=hook_sleep_seconds)


class TopicMeta:
    """Topic metadata item used by fake admin metadata responses."""

    def __init__(self, error: object | None = None) -> None:
        self.error = error


class Metadata:
    """Metadata response used by fake admin client."""

    def __init__(self, topics: dict[str, TopicMeta]) -> None:
        self.topics = topics


class FakeAdminClient:
    """AdminClient-like test double returning fixed topic metadata."""

    def __init__(self, topics: list[str] | None = None) -> None:
        configured_topics = topics or ["in", "out", "dlq"]
        self._metadata = Metadata({topic: TopicMeta() for topic in configured_topics})

    def list_topics(self, timeout: float) -> Metadata:
        del timeout
        return self._metadata
