# Aegis Agent and Service Behavior

## Control Plane

The Go Control Plane is deterministic. It owns worker routing, telemetry ingestion, heartbeat expiry, failure rules, incident state, Redis locks, and Kafka event publication. It does not call Slack, PostgreSQL, S3, or the AI model endpoint.

Control Plane membership:

- Each CP registers a TTL lease in Redis.
- Each CP polls the active CP member list.
- Each CP builds the same in-memory consistent hash ring from sorted CP members.
- Redis is not a hash-ring engine.
- Workers never register themselves in Redis.

Owner redirect:

- An Agent starts with bootstrap CP URLs.
- The receiving CP computes the ring owner for `worker_id`.
- If it owns the worker, it accepts the stream.
- If not, it returns a normal network address hint for the owner.
- The Agent tries the hint and falls back to bootstrap URLs when the hint is stale.

Failure boundaries:

- Redis outages block new leases and locks but do not make AI decisions.
- Kafka outages backpressure event publication and surface health metrics.
- Agent disconnects degrade diagnostics and use the last telemetry window.
- Composer and sink failures do not affect CP detection.

## Python GPU Agent

The Agent represents a monitored GPU node, AI worker node, or model-server host. It supports real collectors later and ships with a synthetic collector for local validation.

Synthetic modes:

- normal operation
- VRAM pressure
- overheating
- latency spike
- missed heartbeat
- freeze
- ECC burst
- model-server crash
- network partition
- restart

Diagnostics:

- The Agent keeps a bounded diagnostic buffer of telemetry, logs, events, health state, and simulation state.
- `TriggerDiagnostics` returns JSON containing `worker_id`, `incident_id`, timestamps, telemetry window, logs, failure indicators, health status, and suspected cause hints.

## Postmortem Composer

The Composer consumes diagnostics events, builds a prompt, optionally merges engineer guidance, calls an OpenAI-compatible endpoint, validates Markdown, and publishes generated postmortems. Validation checks required sections, incident IDs, worker IDs, non-empty content, metadata, and obvious hallucinated infrastructure names.

## Sink Workers

Go Sink Workers consume generated postmortems or delivery requests. They deliver concise summaries to Slack, full Markdown and metadata to PostgreSQL, and immutable archives to S3/MinIO. Delivery is idempotent by `incident_id`, and retries move permanently failed records to the DLQ topic.

## Retry Behavior

Agents use Full Jitter Exponential Backoff for owner redirects, stale owner hints, and bootstrap retry. Kafka consumers retry explicit delivery attempts and publish status for success, retry, and DLQ.

## Future Notes

// TODO: Wire production gRPC mTLS once certificate issuance is decided.

# TODO: Redis Pub/Sub or keyspace notifications can later provide faster ring-rebuild hints.

