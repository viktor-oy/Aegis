# ADR 0001: Aegis Runtime Boundaries

Status: accepted

## Context

Aegis watches GPU and AI worker failure signals while staying out of the serving data path. The Control Plane needs deterministic behavior under failover and backpressure. The AI endpoint is useful for composing readable postmortems but is not trustworthy as a failure oracle.

## Decision

- The Go Control Plane detects failures using explicit rules only.
- Redis stores CP membership leases and incident locks, not worker ownership logic.
- CP replicas build in-memory consistent hash rings from CP members.
- Agents discover owners through bootstrap URLs and redirects.
- Kafka is the durable event backbone for diagnostics, postmortems, delivery, retries, and DLQs.
- Kubernetes orchestrates services but does not own business logic.

## Consequences

The system remains portable outside Kubernetes and deterministic during cluster changes. Some adapters are deliberately thin so production teams can replace the MVP local dependencies without touching core detection logic.

