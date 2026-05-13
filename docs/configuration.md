# Runtime Configuration

## Agent

| Variable | Default | Notes |
| --- | --- | --- |
| `AEGIS_WORKER_ID` | `local-worker` | Stable worker identity. In Kubernetes the DaemonSet uses `spec.nodeName`. |
| `AEGIS_TELEMETRY_DATA_SOURCE` | `MOCK` | `GPU` reads local GPU stats through `nvidia-smi`; `MOCK` uses the synthetic collector. |
| `AEGIS_SIMULATION_MODE` | `normal` | Applies only when `AEGIS_TELEMETRY_DATA_SOURCE=MOCK`. |
| `AEGIS_CP_BOOTSTRAP_URLS` | `aegis-control-plane:50051` | Comma-separated bootstrap URLs. Agents still follow owner redirects. |
| `AEGIS_HEARTBEAT_INTERVAL_SECONDS` | `5` | Agent emission cadence. |

## Composer

| Variable | Default | Notes |
| --- | --- | --- |
| `AEGIS_KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap servers. |
| `AEGIS_COMPOSER_GROUP_ID` | `aegis-postmortem-composer` | Consumer group for diagnostics events. |
| `AEGIS_COMPOSER_INPUT_TOPIC` | `aegis.diagnostics.collected` | Diagnostics topic consumed by Composer. |
| `AEGIS_COMPOSER_OUTPUT_TOPIC` | `aegis.postmortem.generated` | Generated postmortem topic consumed by Sink Workers. |
| `AEGIS_LLM_BASE_URL` | `http://vllm.aegis-system.svc.cluster.local:8000` | OpenAI-compatible inference endpoint, for example vLLM or KServe. |
| `AEGIS_LLM_MODEL` | `meta-llama/Llama-3.1-8B-Instruct` | Model name passed to `/v1/chat/completions`. |
| `AEGIS_LLM_API_KEY` | unset | Optional bearer token. |

## Sink Workers

| Variable | Default | Notes |
| --- | --- | --- |
| `AEGIS_POSTGRES_DSN` | unset | PostgreSQL DSN from Kubernetes Secret. |
| `AEGIS_SLACK_WEBHOOK_URL` | unset | Slack or compatible webhook from Kubernetes Secret. |
| `AEGIS_S3_BUCKET` | `aegis-postmortems` | Bucket for immutable postmortem archives. |

## Control Plane

| Variable | Default | Notes |
| --- | --- | --- |
| `AEGIS_CP_ID` | `cp-0` | Stable CP replica ID. |
| `AEGIS_CP_ADDRESS` | `aegis-control-plane:50051` | Address returned in owner redirect hints. |
| `AEGIS_HTTP_ADDRESS` | `:8080` | Health endpoint bind address. |

// FIXME: the MVP in-memory adapters document the domain behavior; production Redis/Kafka/gRPC adapters should be wired behind the same interfaces before load testing.

