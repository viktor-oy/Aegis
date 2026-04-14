# Aegis System Postmortem

## Project Goal

Aegis was built to detect GPU/AI worker failure signals, coordinate diagnostics, generate postmortems, and deliver them to operational sinks without placing the control plane in the inference serving path.

## Architecture

Python Agents stream telemetry to Go Control Plane replicas. CP replicas use Redis leases to build an in-memory hash ring, acquire Redis incident locks, and publish diagnostic events to Kafka. The Python Composer calls a black-box OpenAI-compatible endpoint and publishes Markdown postmortems. Go Sink Workers fan out to File paths and Email.

### Environment

The validation setup uses local Kubernetes with pinned infrastructure versions, configurable GPU or mock telemetry, an OpenAI-compatible inference endpoint, Redis, Kafka, and Mailpit.

## Failures and Bottlenecks

The most interesting failure boundary is ownership churn. CP failover requires Redis TTL expiry, ring rebuild, Agent reconnect, and idempotent incident handling to converge without duplicate postmortems.

Kafka backlog is expected when Composer or sinks are degraded. That backlog must be visible through lag metrics and KEDA scaling signals.

## Redis Lease Behavior

Redis is used for CP leases and incident locks only. TTL expiry removes stale CP members. Ring rebuilds are triggered by membership fingerprint changes during polling.

## Owner Redirects

Agents never build the ring. They retry bootstrap URLs, accept owner hints, and fall back when a hint is stale.

## Degraded Diagnostics

If an Agent is unreachable after the CP detects a failure, the CP emits a partial or unavailable diagnostic event using the last known telemetry window. The Composer must label the postmortem as degraded.

## AI Failure Behavior

AI failures should not affect CP detection. Composer validation rejects malformed output and can publish a degraded postmortem with raw diagnostic context.

## Sink Failure Behavior

Sink Workers retry delivery, publish status, and move permanent failures to DLQ. File writes are keyed by deterministic `incident_id` for idempotency.

## Tradeoffs

The MVP favors deterministic behavior and clear failure boundaries over deeply optimized transport adapters. Real Redis/Kafka clients can be swapped behind the adapter interfaces without changing domain logic.

## Future Improvements

- Persist compact worker heartbeat snapshots in Redis for faster CP failover recovery.
- Add production gRPC mTLS.
- Expand GPU vendor collectors.
- Add an Operator/CRD for managed cloud deployments.
