# Aegis MVP Release Notes

Date: 2026-05-13

## Delivered

- Monorepo foundation with pinned tool and dependency ledger.
- Go Control Plane domain core for consistent hashing, heartbeat expiry, deterministic detection, incident IDs, Redis-style leases/locks, bounded queues, and event envelopes.
- Python Agent with configurable `GPU` or `MOCK` telemetry source, synthetic failure modes, Full Jitter Backoff, redirects, and bounded diagnostics.
- Python Composer that consumes diagnostics from Kafka, calls an OpenAI-compatible inference endpoint, validates Markdown, and publishes generated postmortem events to Kafka.
- Go Sink Workers for Slack, PostgreSQL, and S3/MinIO-style delivery with retries, idempotency, status, and DLQ routing.
- Cloud-targeted Helm/Kustomize assets, local Tilt loop, Dockerfiles, optional Terraform, KEDA scaling, NetworkPolicy, and OpenTelemetry Collector configuration.
- Unit, integration, E2E contract, and chaos/performance contract tests with report artifact locations.

## Known Follow-Ups

- Replace in-memory MVP adapters with production Redis, Kafka, and gRPC transport adapters behind the existing interfaces.
- Add real GPU vendor collectors beyond `nvidia-smi`.
- Wire production mTLS once certificate issuance is selected.
- Run chaos scenarios against the Kind/Tilt stack and update the human-written system postmortem with observed metrics.

