# Aegis Kafka Event Reference

All Kafka events use a common JSON envelope. Topics are defined in
[`infra/kafka/topics.yaml`](../../infra/kafka/topics.yaml).

---

## Common Envelope Schema

Every event published to any Aegis Kafka topic uses this envelope:

```json
{
  "event_id":       "<random-hex-32>",
  "event_type":     "<string>",
  "incident_id":    "<inc_sha256[:24]>",
  "worker_id":      "<string>",
  "producer":       "<service-name>/<instance-id>",
  "timestamp":      "<RFC3339-UTC>",
  "schema_version": "aegis.events.v1",
  "correlation_id": "<string>",
  "causation_id":   "<event_id-of-parent | empty>",
  "payload":        { ... }
}
```

| Field | Description |
|-------|-------------|
| `event_id` | Random 128-bit hex string; unique per publication. Not used as primary identity. |
| `event_type` | Mirrors the topic name (e.g. `aegis.incident.detected`). |
| `incident_id` | Deterministic: `inc_` + first 24 hex chars of `SHA256(worker_id | failure_type | time_bucket)`. |
| `worker_id` | Stable worker identifier from the GPU Agent. |
| `producer` | Publishing service plus replica ID. |
| `timestamp` | UTC time of publication. |
| `schema_version` | Always `aegis.events.v1` for the current schema. |
| `correlation_id` | End-to-end trace identifier propagated from gRPC through sinks. |
| `causation_id` | `event_id` of the event that triggered this one; empty for the root event. |
| `payload` | Event-specific body described per topic below. |

---

## Topics

### aegis.incident.detected

**Published by**: Go Control Plane  
**Partitions**: 12 | **Retention**: 7 days

Emitted when the CP confirms ownership, acquires the Redis incident lock,
and transitions the worker to `SUSPECTED`.

**Payload**:
```json
{
  "reason":       "<human-readable detection reason>",
  "failure_type": "missed_heartbeat | gpu_overheat | vram_pressure | ecc_burst | model_unhealthy | latency_spike | synthetic_failure",
  "severity":     "warning | critical"
}
```

---

### aegis.diagnostics.requested

**Published by**: Go Control Plane  
**Partitions**: 12 | **Retention**: 7 days

Emitted immediately after `aegis.incident.detected`. Signals that the CP has
called (or is about to call) `TriggerDiagnostics` on the agent.

**Payload**:
```json
{
  "diagnostic_status": "requested"
}
```

---

### aegis.diagnostics.collected

**Published by**: Go Control Plane  
**Partitions**: 12 | **Retention**: 7 days

Emitted after `TriggerDiagnostics` returns (or times out). Carries the
diagnostic bundle whether complete or unavailable.

**Payload**:
```json
{
  "diagnostic_status": "complete | partial | unavailable",
  "diagnostics": {
    "telemetry_window": [...],
    "logs":             [...],
    "failure_indicators": { ... },
    "gpu_health":        { ... },
    "model_health":      { ... },
    "agent_health":      { ... },
    "suspected_cause":   "<string>"
  }
}
```

---

### aegis.postmortem.requested

**Published by**: Go Control Plane  
**Partitions**: 12 | **Retention**: 14 days

Emitted after diagnostics are collected. Triggers the Python Composer to
generate a Markdown postmortem.

**Payload**:
```json
{
  "diagnostic_status": "complete | partial | unavailable",
  "postmortem_format": "default"
}
```

Optional field `engineer_guidance` (string) may be merged in by an engineer
before the Composer picks up the event.

---

### aegis.postmortem.generated

**Published by**: Python Composer  
**Partitions**: 12 | **Retention**: 14 days

Emitted after the Composer validates and publishes the AI-generated Markdown
postmortem.

**Payload**:
```json
{
  "postmortem_markdown": "<full Markdown string>",
  "model_used":          "<endpoint identifier>",
  "generated_at":        "<RFC3339-UTC>",
  "validation_passed":   true
}
```

---

### aegis.postmortem.delivery.requested

**Published by**: Python Composer  
**Partitions**: 12 | **Retention**: 14 days

Dispatches delivery work to Go Sink Workers for Slack, File, and Email.

**Payload:**

```json
{
  "postmortem": "# Incident...",
  "targets": ["file", "email"],
  "format": "markdown"
}
```

---

### 7. aegis.postmortem.delivery.status

Emitted by Sink Workers when a delivery attempt succeeds or fails.

**Payload:**

```json
{
  "target":       "file | email",
  "status":       "SUCCESS | RETRY | DLQ",
  "reference":    "<File path | Email ID>",
  "error":        "<error string or null>"
}
```

---

### aegis.postmortem.delivery.retry

**Published by**: Go Sink Workers  
**Partitions**: 6 | **Retention**: 14 days

Emitted when a delivery attempt fails and a retry is scheduled. Consumers
apply exponential backoff before re-publishing to `delivery.requested`.

---

### aegis.postmortem.delivery.dlq

**Published by**: Go Sink Workers  
**Partitions**: 3 | **Retention**: 30 days

Final resting place for events that exhausted all retry attempts.
Operations teams monitor this topic to manually replay or escalate.

---

## S3/MinIO Object Layout

Postmortems stored by the S3 Sink Worker follow this key scheme:

```
postmortems/{worker_id}/{incident_id}/postmortem.md
postmortems/{worker_id}/{incident_id}/metadata.json
```

Delivery is idempotent by `incident_id`. Re-uploading the same key is safe
(object overwrite) and is the intended retry behaviour.

---

## Failure Type Values

| Value | Detection trigger |
|-------|-------------------|
| `missed_heartbeat` | Heartbeat deadline expired in the CP min-heap |
| `gpu_overheat` | Temperature ≥ 85 °C sustained across ≥ 3 samples |
| `vram_pressure` | VRAM ratio ≥ 92% sustained across ≥ 3 samples |
| `ecc_burst` | ECC error count delta ≥ 8 within the window |
| `model_unhealthy` | `model_server_healthy == false` on any single sample |
| `latency_spike` | P99 latency ≥ 2500 ms sustained across ≥ 3 samples |
| `synthetic_failure` | `synthetic_failure` field non-empty and not `"normal"` |

See [`docs/api/grpc.md`](grpc.md) for the telemetry fields that drive these rules.
