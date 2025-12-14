# Aegis Repository Instructions

You are working in the Aegis monorepo. Keep changes small, reviewable, and aligned with the architecture boundaries below.

## Boundaries

- Go Control Plane owns deterministic failure detection and must not call AI, Slack, PostgreSQL, or S3.
- Python Composer may call the AI endpoint only after CP-produced diagnostics events.
- Python Agent must not talk to Redis or build the CP ring.
- Kubernetes orchestrates services but business logic must not import Kubernetes APIs.
- Kafka is the durable event backbone for asynchronous work.

## Coding Rules

- Prefer small adapter interfaces around Kafka, Redis, AI, Slack, PostgreSQL, and S3.
- Pin dependencies and image tags. Never use `latest`.
- Keep TODO/FIXME comments meaningful and rare.
- Preserve correlation IDs across gRPC, Kafka, Composer, and sinks.
- Use semantic commits: `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `refactor:`, `perf:`.

## Commands

```sh
make test-python
make test-go
make lint
make proto
tilt up
```

## Tests

- Unit tests go under `tests/unit/` for Python and near Go packages for Go internals.
- Integration tests go under `tests/integration/` and write artifacts to `/reports/integration/`.
- E2E tests go under `tests/e2e/` and stay limited to critical full-system workflows.
- Chaos/performance tests go under `tests/chaos/` and write raw artifacts to `/reports/chaos/`.

## Documentation

README starts with one overarching Mermaid diagram. Update `VERSION_LEDGER.md` when changing any pinned runtime, image, CLI, or major dependency. Update `agents.md` when changing service ownership, retry behavior, failure boundaries, or redirect logic.

