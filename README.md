# Aegis

## Overview

Aegis is a watchdog control plane for your GPU/AI infra. It sits completely out-of-band so it never bottlenecks your actual inference requests. Python agents sit on the GPU workers streaming heartbeats and telemetry to a Go control plane. The Go backend handles all the heavy lifting: sharding, deterministic failure detection, acquiring Redis locks, and publishing incident events to Kafka. Finally, a downstream Composer service spits out a neat postmortem and hands it to sink workers for delivery.

We treat inference servers (vLLM, KServe, Triton), file paths, and Email SMTP as external, black-box systems. Aegis integrates with them through narrow adapters. The golden rule: we never let a flaky AI endpoint dictate whether a worker is actually dead.

## Problem Statement

AI infra is messy. GPUs overheat, VRAM blows up, model servers crash, and networks randomly partition. We needed a deterministic way to catch these failures instantly, grab diagnostic context before it's lost, and fan out a useful postmortem—all without letting our observability tools couple tightly to our delivery systems.

## Architecture Summary

- Python Agents lock onto stable `worker_id`s and connect to the Control Plane (CP) via bootstrap URLs.
- The Go CPs register themselves in Redis and build a shared, in-memory consistent hash ring.
- If an agent hits the wrong CP, the CP simply returns a network redirect hint to the rightful owner.
- Worker state follows a strict state machine: `HEALTHY -> SUSPECTED -> DIAGNOSTICS_TRIGGERED -> DIAGNOSTICS_COLLECTED -> POSTMORTEM_REQUESTED -> POSTMORTEM_GENERATED -> DELIVERY_IN_PROGRESS -> DELIVERED/DELIVERY_FAILED -> RESOLVED`.
- Kafka acts as the durable spine, carrying all incident, diagnostic, postmortem, retry, and DLQ events wrapped in a common envelope.
- The Composer only reaches out to an OpenAI-compatible endpoint *after* we've deterministically caught an incident.
- Go Sink Workers handle the final mile: dumping postmortems to local files or Email via SMTP, complete with idempotency, retries, and dead-letter queues.

## Cloud Deployment Notes

Built for K8s. We use Helm and Kustomize to spin up the standalone Aegis services alongside Kafka, Redis, PostgreSQL, MinIO, a mock Slack webhook, and an OpenAI-compatible LLM endpoint (like vLLM). We lean on Kubernetes for the boring stuff (DNS, scheduling, secrets, autoscaling) so Aegis code can focus entirely on shard routing, failure detection, state transitions, and correlation propagation.

## Local Validation

How to run this beast locally:

1. Your developer filesystem stores the repository.
2. We rely heavily on [mise](https://mise.jdx.dev/) to pin our toolchains and CLIs so nobody has "works on my machine" issues.
3. Kind, Minikube, or k3d handles the actual Kubernetes workloads.

After running `mise install`, here are your lifesavers:

```sh
make dev-up            # Starts infra (Terraform) and aegis apps (Tilt)
make dev-up-0          # Full nuclear teardown (destroys Kind cluster) and recreates everything
make dev-up-0-lite     # Fast clean slate: forcefully wipes K8s namespaces without destroying the underlying cluster
```

> [!NOTE]
> **Hack / Gotcha: Fast Teardowns without Finalizer Deadlocks**
> If you've ever had a cluster hang during teardown, you know the pain of Kubernetes finalizers deadlocking when a Kubelet refuses to release a volume mount. The `dev-up-0-lite` command intentionally wipes active workloads (Pods/StatefulSets) *before* touching persistent volumes (PVCs) and ConfigMaps to bypass this nightmare entirely.

> [!WARNING]
> **Gotcha: The "More is Better" Trap (Resource Contention)**
> It's super tempting to bump the replica counts in your `values-local.yaml` to watch Aegis scale, but remember you're running all of this on a local Docker VM or Colima. If you aggressively crank up the replicas for Kafka, Control Planes, or the LLM endpoint, your host machine will choke hard. Services will mysteriously crash-loop, Kafka connections will drop, and pods will hang forever—all due to pure compute/memory starvation, not code bugs. If things start acting weird, dial back your replicas or bump your Docker VM's RAM allocation!

Tilt applies the Helm/Kustomize artifacts. We use live update rules so your Python tweaks sync instantly without full image rebuilds, Go changes only recompile the affected binary, and stateful infra doesn't constantly reboot on app edits. `Tiltfile.infra` handles the heavy backing services (Kafka, Redis) and can be spun up independently for integration testing.

## Services

- `aegis-control-plane`: The Go brains. Handles ownership, ingestion, failure detection, incident locks, diagnostics, and Kafka publication. Configured via `AEGIS_CP_ADDRESS`, `AEGIS_GRPC_ADDRESS`, `AEGIS_REDIS_ADDR` and `AEGIS_KAFKA_BROKERS`. It fully supports multicore telemetry consumption.
- `aegis-agent`: The Python GPU/AI worker monitor. We packed it with synthetic failure modes, a diagnostics buffer, and a local FastAPI Swagger UI so you can test API endpoints directly.
- `aegis-composer`: The Postmortem Composer (Python, FastAPI, Kafka). It grabs diagnostics, formats them via an OpenAI-compatible endpoint, validates the Markdown, and throws the generated postmortem back into Kafka. Also exposes a synchronous API endpoint for quick testing.

#### Composer Configuration
- `AEGIS_COMPOSER_GROUP_ID`: Kafka consumer group for Composer.
- `AEGIS_COMPOSER_INPUT_TOPIC`: Kafka topic Composer consumes diagnostics from.
- `AEGIS_COMPOSER_OUTPUT_TOPIC`: Kafka topic Composer publishes postmortems to.

#### Prompt Injection Overrides
Composer embeds its default system architecture and formatting prompts from the internal `/workspace/services/composer/prompts/` directory. You can override these by passing raw markdown strings into these env vars:
- `AEGIS_COMPOSER_SYS_ARCH_PROMPT`: Overrides the system architecture context.
- `AEGIS_COMPOSER_FORMAT_PROMPT`: Overrides the LLM formatting guidelines.

Alternatively, you can volume mount your custom markdown files directly into the `/workspace/services/composer/prompts/` directory (e.g., overriding `postmortem-format.md` or `system-architecture.md`) to replace the default prompts without using environment variables.

- `aegis-sink`: The Go Kafka consumer that handles the final delivery of postmortems to File systems and Email.
- `mock-slack`: A dummy webhook receiver we use for local testing (not for production).

## Kafka Topics

- `aegis.incident.detected`
- `aegis.diagnostics.requested`
- `aegis.diagnostics.collected`
- `aegis.postmortem.requested`
- `aegis.postmortem.generated`
- `aegis.postmortem.delivery.requested`
- `aegis.postmortem.delivery.status`
- `aegis.postmortem.delivery.retry`
- `aegis.postmortem.delivery.dlq`

## Workflows

**Postmortem workflow:**

```text
Agent -> gRPC telemetry -> CP -> deterministic incident -> Kafka -> Composer -> AI endpoint -> Kafka generated postmortem
```

**Sink fanout workflow:**

```text
Kafka generated postmortem -> Go Sink Workers -> File + Email -> status/retry/DLQ topics
```

## Observability

Every service spits out standard structured logs to stdout. We pass strict trace correlation IDs across gRPC, Kafka, Composer, the Sinks, and even CP owner redirects so you never lose the thread.

Locally, everything just dumps to stdout.

## Disaster Simulation (Scenario Runner)

End-to-End tests are great for binary CI/CD pipelines, but they absolutely suck for demonstrating system resilience at massive async scale. 
To actually prove Aegis's fault tolerance, we built a **Scenario Runner**.

Instead of a rigid test, the Scenario Runner is a chaos script that injects staggered faults into a heavily scaled cluster (e.g., 10 CPs, 30 Agents) on a delay timer. You literally get to sit back and watch the system dynamically rebalance and recover in real-time.

### Example: Sustained High Temperature
1. **Trigger the disaster:**
   ```bash
   # (Placeholder: Script to be implemented in scripts/simulate_disaster.py)
   python3 scripts/simulate_disaster.py --scenario=temperature --incident-id=temp-spike-001
   ```
2. **Observe the system react:**
   👉 [Click here to view the live filtered logs for this incident in Tilt](http://localhost:10350/r/(all)/overview?q=temp-spike-001)

> [!TIP]
> **Engineering Decision: Unified Observability UX**
> Pre-linking Tilt URLs with regex queries (like the link above) completely eliminates cognitive load. It instantly cuts through the noise of 50 background pods so you can perfectly track a single `correlation_id` hopping from Agent -> CP -> Kafka -> Composer -> Sink.

## Testing

```sh
make test-python
tilt up -f Tiltfile.infra # Ensure backing services are up for integration tests
make test-go
```

Root-level test layout:

- `tests/unit/`: fast, isolated unit tests.
- `tests/integration/`: component-boundary tests. These write artifacts straight to `/reports/integration/`.
- `tests/e2e/`: a handful of full-system scenarios.
- `tests/chaos/`: resilience and performance stress tests, dumping artifacts to `/reports/chaos/`.

> **Note on Integration Testing State:** Our integration tests need the infra cluster running (`make dev-up-0-lite` or `tilt up -f Tiltfile.infra`). The tests handle their own state cleanup before executing. To keep things blazing fast, Kafka state wiping is surgically scoped *only* to the topics touched by the specific test (e.g., `kafka:aegis.incident.detected,aegis.diagnostics.requested`).

### Engineering Decisions & Test Infrastructure

1. **Default Worker ID Formatting**: The default `Worker ID` for agents is intentionally formatted as a composite key (`$(POD_NAMESPACE)--$(POD_NAME)`) injected via the Downward API, rather than a raw pod name or UUID. This allows the SRE/developer reading the generated postmortem to instantly identify the namespace and specific node pod that failed, speeding up incident response times.

To keep testing consistent, output clean, and performance highly optimized across local/CI, we enforce a few strict conventions:

1. **Go Test Runner (`gotestsum`)**: We don't natively run `go test` because it can be messy. The `Makefile` auto-downloads `gotest.tools/gotestsum@latest` for all Go tests. You get beautifully formatted, colorized output and clean summaries, and we pass `--format standard-verbose` to make sure logs still stream in real-time.
2. **Python Test Runner (`pytest`)**: Our Python `Makefile` targets enforce `--color=yes` and `--log-cli-level=INFO` because nobody likes reading raw monochrome logs.
3. **Optimized Kafka State Wiping**: Wiping state between tests by deleting and recreating Kafka topics is agonizingly slow. Instead, we use a custom Python script (`scripts/wipe_infra_state.py`) that queries `infra/kafka/topics.yaml` for partition limits and securely deletes records for *exactly* the scoped topics via `kafka-delete-records.sh` (e.g. `"kafka:aegis.postmortem.generated"`). It's easily 100x faster than tearing down topics or bouncing JVM containers.
4. **Uniform Log Highlighting**: All internal test-progress prints (in both Python and Go) are normalized to use a bold `\033[36m[TEST: ...]\033[0m` Cyan prefix via unified helpers (`tests/integration/testutils/infra.go:LogInfo` and `scripts/wipe_infra_state.py:log_info`). This makes it trivially easy to spot your test boundaries inside the noisy async app logs.
5. **Isolated Integration Test Cluster**: To stop state corruption and port collisions from ruining your local development, all integration tests target a dedicated Kind cluster (`aegis-intg-test`) with host ports dynamically bound via `AEGIS_ENV=intg-test`. You don't manage this: invoking `make test-intg` fires up our DRY `tests/integration/testutils/ensure_test_infra.py` wrapper. It auto-provisions the cluster, murders any zombie processes hogging test ports, spins up `tilt` in the background, waits for services (including the heavy LLM) to get healthy, and initializes Kafka topics before the test runner ever executes.

   > [!WARNING]
   > When `make test-intg` natively invokes `kind create cluster` behind the scenes, Kind's hardcoded default behavior is to automatically switch your active `kubectl` context to the newly created cluster. If you run tests in a background tab, be aware that your active terminal context might unexpectedly switch on you!
   
   > **Hack / Gotcha (inotify limits):** We run ~15 microservices concurrently during testing, so Tilt tries to stream logs for all of them at once. This instantly blows past the default Linux `fs.inotify.max_user_instances` limit (128) on the Kind node, throwing annoying `failed to create fsnotify watcher: too many open files` errors. Instead of forcing everyone to manually bump sysctl limits on their Mac or Docker VM, the `ensure_test_infra.py` wrapper intercepts the cluster boot process and sneaks in a dynamic sysctl hack (`fs.inotify.max_user_instances=512`) directly inside the Kind node container before handing off to Tilt. It works flawlessly.

### How-to: Running Specific Tests

To run tests individually while leveraging all our custom formatting and state clearing wrappers:

- **Run all tests**: `make test-go test-agent test-composer`
- **Run all integration tests**: `make test-intg`
- **Start test infrastructure with HUD**: `make tilt-test-infra-up` (can be run manually before `make test-intg` to view the infrastructure spin-up via Tilt's interactive UI on port 10352)
- **View integration test infra logs**: `make test-tilt-logs`
- **Clear test ports manually**: `make clear-test-ports` (terminates any zombie processes occupying test infra ports)
- **Run specific Go integration test**: `go run gotest.tools/gotestsum@latest --format standard-verbose -- ./services/sink -run TestSinkServiceIntegration_FileSink`
- **Run specific Python integration test**: `PYTHONPATH=.:gen/python python3 -m pytest -s --color=yes --log-cli-level=INFO tests/integration/agent/test_agent_integration.py::test_grpc_stream_accepted`
  > **Note for Composer Tests:** The Composer integration test (`test_composer_integration.py`) generates a real postmortem using the local LLM and writes the resulting markdown file to `docs/services/composer/local/artifacts/` for manual inspection.
- **Wipe infra state manually**: `AEGIS_KUBE_CONTEXT="kind-aegis" make wipe-infra-state TARGETS="redis,kafka:aegis.telemetry,aegis.events"` (You can specify exact Kafka topics as subtargets. The `AEGIS_KUBE_CONTEXT` environment variable governs which cluster is targeted; you MUST provide it explicitly. If wiping your dev cluster, pass `AEGIS_KUBE_CONTEXT="kind-aegis"`. If wiping tests, pass `AEGIS_KUBE_CONTEXT="kind-aegis-intg-test"`).

**Useful Flags:**
- `VERBOSE=1` (e.g., `make test-intg VERBOSE=1`): Instructs the underlying test runners (`pytest`, `gotestsum`) to stream all debug output and inner service logs dynamically as they run. By default (`VERBOSE=0`), integration tests run in a quiet mode and only print the results of the tests themselves to keep your terminal clean.
- `AEGIS_KUBE_CONTEXT` (No default): Used universally by commands like `make init-kafka` and `make wipe-infra-state` to determine the active cluster target. This must be provided explicitly because these commands can be highly destructive to state.
## KEDA Scaling

KEDA ScaledObjects look at CP queue depth and active agents for the Control Plane, Kafka lag for Composer and Sink Workers, and latency/concurrency signals for the optional local AI server. CPU and memory HPAs are kept around as fallback scalers.

## Helm, Kustomize, and Deployment Environments

Helm owns the reusable cloud chart under `infra/helm/aegis`. We employ a hybrid architecture where Kustomize orchestrates the rendering of the Helm chart for specific environments, located under `infra/kustomize/overlays/`.

### Engineering Judgment on Production Defaults
We maintain explicit Kustomize overlays and `values-<env>.yaml` files for `local`, `dev`, and `staging` environments. However, **we intentionally omit a default `values-prod.yaml` for production**. 
This is a conscious security and engineering decision to ensure that default configurations (such as weak passwords, open NodePorts, or debug modes) are never accidentally pushed to production. An engineer or an automated CI/CD pipeline must explicitly provide a hardened production values file to successfully deploy to production.

### Usage Guides

The infrastructure deployment instructions are split into two categories:

#### 1. Development Use Case

For active development, contributing, and testing natively against the source repository, we use a combination of Terraform, Tilt, and Kustomize to orchestrate the Helm chart across environments.

**Local Development (with Live Updates)**:
We use a wrapper command that first provisions the local Kind cluster via Terraform and then attaches Tilt for live updates.
```sh
make dev-up
```

*Note: If you wish to manage the cluster without Terraform, you can use the manual fallbacks `make kind-up-manual` and `make tilt-up-manual`.*

**Manual Local Deployment**:
```sh
kustomize build --enable-helm infra/kustomize/overlays/local | kubectl apply -f -
```
**Development & Staging Environments**:
```sh
kustomize build --enable-helm infra/kustomize/overlays/dev | kubectl apply -f -
# OR
kustomize build --enable-helm infra/kustomize/overlays/staging | kubectl apply -f -
```
**Production Kustomize Deployment**:
Since there are no default production values, supply your own `values-prod.yaml` first:
```sh
cat <<EOF > infra/kustomize/overlays/prod/values-prod.yaml
# Production overrides go here
EOF
kustomize build --enable-helm infra/kustomize/overlays/prod | kubectl apply -f -
```

#### 2. Deployment Use Case

If you are a consumer deploying Aegis via the hosted Helm package store (e.g., assuming it is hosted at `https://charts.aegis.io`), use standard Helm commands:

1. Add the Helm repository:
```sh
helm repo add aegis https://charts.aegis.io
helm repo update
```

2. Create your hardened `values-prod.yaml` securely.

3. Deploy the chart to your cluster:
```sh
helm install aegis-production aegis/aegis --namespace aegis-system --create-namespace -f values-prod.yaml
```


Helm owns the reusable cloud chart under `infra/helm/aegis`. Kustomize overlays under `infra/kustomize/overlays/local` and `infra/kustomize/overlays/cloud` inject small environmental differences. Terraform under `infra/terraform/local` is strictly optional and just provisions the foundation; Aegis expects to deploy into an existing cluster without relying on Terraform.

## Technologies Used

**Languages and protocols:** Go, Python 3.14.1, protobuf, gRPC, JSON event envelopes, Markdown.
**Distributed systems:** consistent hashing with virtual nodes, Redis TTL membership leases, Redis incident locks, bounded queues, min-heap heartbeat expiry, deterministic incident IDs, idempotent Kafka consumers, retry and DLQ topics.
**Infrastructure:** Kubernetes, Helm, Kustomize, Kind, Minikube/k3d-compatible overlays, Tilt, KEDA, Terraform, mise.
**Data and messaging:** Kafka, Redis, MinIO for local validation.
**Observability:** trace correlation IDs.
**AI integration:** OpenAI-compatible inference endpoint (vLLM or KServe).
**Security and hardening:** Kubernetes Secrets, optional gRPC mTLS wiring, Kafka authentication and ACL notes, Redis authentication, NetworkPolicy, least-privilege ServiceAccounts, rate limiting, secret rotation notes.

*(Version pins are tightly tracked in `VERSION_LEDGER.md`)*

## API Reference

- [`docs/api/grpc.md`](docs/api/grpc.md) — gRPC services, messages, and connection notes
- [`docs/api/kafka.md`](docs/api/kafka.md) — Kafka topics, envelope schema, and payload shapes
