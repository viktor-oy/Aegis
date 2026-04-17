# Security and Production Hardening

## gRPC mTLS

The MVP keeps gRPC service code independent of a certificate provider. Production deployments should mount server and client certificates through Kubernetes Secrets and enable mTLS at the gRPC adapter layer.

// TODO: choose cert-manager or platform-issued SPIFFE identities before making mTLS mandatory.

## Kafka Authentication and ACLs

Production Kafka should require TLS/SASL and explicit ACLs:

- Control Plane: produce to incident, diagnostics, and postmortem-request topics.
- Composer: consume diagnostics, produce generated postmortems.
- Sink Workers: consume generated postmortems and produce delivery status, retry, and DLQ records.

## Redis Authentication

Redis should require authentication and network access only from Control Plane pods. Agents, Composer, and Sink Workers must not connect to Redis.

## Secrets

Kubernetes Secrets hold PostgreSQL DSNs, S3 credentials, Slack webhook URLs, Redis credentials, Kafka credentials, and inference endpoint API keys. Rotate secrets by creating a new Secret revision, rolling the dependent workloads, and retiring the old value after consumers reconnect.

## Network Policy

The chart includes a baseline NetworkPolicy. Production overlays should restrict:

- Agent egress to Control Plane gRPC and OTel.
- Control Plane egress to Redis, Kafka, Agents for diagnostics, and OTel.
- Composer egress to Kafka, inference endpoint, and OTel.
- Sink Worker egress to Kafka, Slack, PostgreSQL, S3, and OTel.

## Least Privilege

Each Aegis workload has a dedicated ServiceAccount. The MVP does not require Kubernetes API access from business logic, so RBAC should remain minimal.

## Rate Limiting and Backpressure

Control Plane telemetry ingestion uses bounded queues and returns `ResourceExhausted` when saturated. Agents use Full Jitter Exponential Backoff on reconnect and redirects. Sink Workers publish retry and DLQ status instead of silently dropping failed deliveries.

