# sdx_core

Shared Python runtime utilities for internal microservices.

## Package contents

- `sdx_core.esri`: ESRI feature service client, token handling, retry policy, and domain exceptions.
- `sdx_core.circuit_breaker`: framework-agnostic async circuit breaker with pluggable storage and listener hooks.
- `sdx_core.logging`: structured logging helpers.
- `sdx_core.settings`: base settings helpers for env-var configuration.
- `sdx_core.retry`: retry policy primitives and interruptible backoff helpers.
- `sdx_core.headers`, `sdx_core.readiness`, `sdx_core.replay_counter`, `sdx_core.errors`: shared utility modules.

## Example usage

- [`qali-internal-geocode-svc`](https://github.com/Spatial-Information-QLD/qali-internal-geocode-svc) is a consumer of `sdx_core` and demonstrates usage of the ESRI client, circuit breaker, and logging/runtime helpers in a production microservice.

## Development

Install dependencies:

```bash
task install
```

Run package tests:

```bash
task test
```

Run checks:

```bash
task code:check
```

Format + fix code:

```bash
task code
```
