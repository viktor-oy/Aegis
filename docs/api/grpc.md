# Aegis gRPC API Reference

Generated from [`proto/aegis/v1/aegis.proto`](../../proto/aegis/v1/aegis.proto).

---

## Services

### ControlPlaneTelemetry

The primary interface between GPU Agents and the Control Plane. Agents open a
bidirectional stream to send telemetry and receive directives. The Control Plane
uses this stream to track heartbeats, detect failure signals, and return
ownership redirects when a replica is not the authoritative owner of a worker.

**Package**: `aegis.v1`
**Default port**: `50051` (TCP, gRPC)
**Transport**: unencrypted in MVP; mTLS is a future TODO once certificate issuance is decided.

| RPC | Request | Response | Type |
|-----|---------|----------|------|
| `StreamTelemetry` | `stream AgentTelemetry` | `stream ControlPlaneDirective` | Bidirectional streaming |
| `SubmitDiagnostics` | `DiagnosticBundle` | `Ack` | Unary |

#### StreamTelemetry

Agents call this RPC immediately after connecting. The stream stays open for
the lifetime of the agent-to-CP session.

**Behaviour**:
- The agent sends `AgentTelemetry` messages at a configured interval (heartbeat).
- The CP responds with a `ControlPlaneDirective` for each received message.
- If the CP is not the authoritative owner of `worker_id`, it returns a
  `redirect` directive containing `owner_hint` (the owning replica's address).
- If the ingest queue is full, the CP closes the stream with
  `codes.ResourceExhausted`.
- Agent disconnection causes the heartbeat deadline for that `worker_id` to
  stop refreshing; the CP will eventually mark the worker as `SUSPECTED` via
  the min-heap expiry loop.

**Error codes**:

| Code | Meaning |
|------|---------|
| `codes.ResourceExhausted` | Ingest queue is full; agent should back off |
| `codes.Internal` | Unexpected processing error |

#### SubmitDiagnostics

Agents may proactively push a diagnostic bundle. This is complementary to the
CP-initiated `TriggerDiagnostics` flow on `AgentDiagnostics`.

**Validation**: `worker_id` and `incident_id` are required; returns
`codes.InvalidArgument` if either is empty.

---

### AgentDiagnostics

A callback interface exposed by the **Python GPU Agent**, not by the Control
Plane. The CP calls this RPC when it needs to collect fresh diagnostics during
incident handling. The agent is expected to expose this service on its own
endpoint.

| RPC | Request | Response | Type |
|-----|---------|----------|------|
| `TriggerDiagnostics` | `DiagnosticRequest` | `DiagnosticBundle` | Unary |

**Note**: If the agent is unreachable, the CP degrades gracefully — it
synthesises a partial `DiagnosticBundle` with `diagnostic_status: "unavailable"`
and continues the incident flow using the last known telemetry window.

---

## Messages

### AgentTelemetry

Sent by the agent on every heartbeat tick.

| Field | Type | Description |
|-------|------|-------------|
| `worker_id` | `string` | Stable identifier for this GPU/AI worker node. Determines ownership via consistent hash ring. |
| `timestamp` | `string` | RFC 3339 Nano timestamp when the sample was collected. |
| `gpu_utilization` | `double` | GPU compute utilization ratio (0.0–1.0). |
| `vram_used_bytes` | `uint64` | VRAM currently in use, in bytes. |
| `vram_total_bytes` | `uint64` | Total available VRAM, in bytes. |
| `temperature_celsius` | `double` | GPU die temperature in degrees Celsius. |
| `power_watts` | `double` | GPU power draw in watts. |
| `ecc_error_count` | `uint64` | Cumulative ECC error count since agent start. |
| `inference_latency_ms` | `double` | P99 inference latency in milliseconds. |
| `local_queue_depth` | `uint64` | Pending inference requests in the local queue. |
| `model_server_healthy` | `bool` | Whether the model server health check passes. |
| `synthetic_failure` | `string` | Non-empty string activates a synthetic failure mode (for local validation). `"normal"` or empty means healthy. |
| `correlation_id` | `string` | Propagated across gRPC, Kafka, Composer, and sinks for trace correlation. |

### ControlPlaneDirective

Returned by the CP in response to each `AgentTelemetry` message.

| Field | Type | Description |
|-------|------|-------------|
| `directive_type` | `string` | `"accepted"` (telemetry queued) or `"redirect"` (not the owner). |
| `owner_hint` | `string` | Network address of the owning CP replica. Set only when `directive_type == "redirect"`. |
| `message` | `string` | Human-readable description. |
| `correlation_id` | `string` | Echoes the correlation ID from the triggering telemetry message. |

**Agent redirect behaviour**: on receiving `"redirect"`, the agent should
connect to `owner_hint`. If `owner_hint` is unreachable, the agent falls back
to its configured bootstrap CP URLs with full-jitter exponential backoff.

### DiagnosticRequest

Sent by the CP to trigger on-demand diagnostics.

| Field | Type | Description |
|-------|------|-------------|
| `worker_id` | `string` | Target worker. |
| `incident_id` | `string` | Deterministic incident ID (`inc_<sha256[:24]>`). |
| `failure_type` | `string` | One of: `missed_heartbeat`, `gpu_overheat`, `vram_pressure`, `ecc_burst`, `model_unhealthy`, `latency_spike`, `synthetic_failure`. |
| `requested_at` | `string` | RFC 3339 Nano timestamp of the diagnostic request. |
| `correlation_id` | `string` | Correlation trace ID. |

### DiagnosticBundle

Returned by `TriggerDiagnostics`; also pushed via `SubmitDiagnostics`.

| Field | Type | Description |
|-------|------|-------------|
| `worker_id` | `string` | Worker this bundle belongs to. |
| `incident_id` | `string` | Incident this bundle belongs to. |
| `collected_at` | `string` | RFC 3339 Nano timestamp of collection. |
| `diagnostic_status` | `string` | `"complete"`, `"partial"`, or `"unavailable"`. |
| `json_payload` | `string` | JSON-encoded diagnostic data: telemetry window, logs, failure indicators, GPU/model/agent health, suspected causes. |
| `correlation_id` | `string` | Correlation trace ID. |

### Ack

Returned by `SubmitDiagnostics`.

| Field | Type | Description |
|-------|------|-------------|
| `accepted` | `bool` | `true` if the bundle was accepted. |
| `message` | `string` | Human-readable status. |

---

## Connection Notes

- Agents discover CP addresses via bootstrap URLs (e.g. `AEGIS_CP_ADDRESS`).
- The gRPC health service (`grpc.health.v1.Health`) is registered on the same
  port for Kubernetes readiness and liveness probes.
- Schema version for Kafka events published downstream: `aegis.events.v1`.

See [`docs/api/kafka.md`](kafka.md) for the Kafka event schema that follows
incident detection.
