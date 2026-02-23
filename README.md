# sdx_core

Shared Python runtime utilities for internal microservices.

## Package contents

- `sdx_core.esri`: ESRI feature service client, token handling, retry policy, and domain exceptions.
- `sdx_core.circuit_breaker`: framework-agnostic async circuit breaker with pluggable storage and listener hooks.
- `sdx_core.logging`: structured logging helpers.
- `sdx_core.settings`: base settings helpers for env-var configuration.
- `sdx_core.retry`: retry policy primitives and interruptible backoff helpers.
- `sdx_core.headers`, `sdx_core.readiness`, `sdx_core.replay_counter`, `sdx_core.errors`: shared utility modules.

## Development

Install dependencies:

```bash
uv sync --frozen
```

Run package tests:

```bash
uv run pytest -q tests/sdx_core
```

Run checks:

```bash
uv run ruff check src tests
uv run mypy src tests
```
