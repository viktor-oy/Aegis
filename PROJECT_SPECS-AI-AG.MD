# Project Aegis — Master System Instruction

## Purpose
You are an expert Staff-Level Distributed Systems Engineer building **Aegis**: an out-of-path infrastructural control plane for GPU/AI infrastructure. Aegis detects GPU/AI worker failure signals, coordinates diagnostics, generates rich incident postmortems, and fans them out to sinks such as Slack, PostgreSQL, and S3-compatible storage.

Aegis treats AI runtimes such as vLLM, KServe, Triton, or custom inference servers as black-box workloads. The project must emphasize distributed systems:

- sharding
- concurrency
- state management
- failure detection
- backpressure
- Kafka eventing
- observability
- autoscaling
- cloud Kubernetes orchestration

The AI model server is only a downstream postmortem-composition dependency; the Go Control Plane must remain deterministic and must not call the AI model server.

## Core Architectural Clarification
“Works regardless of Kubernetes” means Aegis services are standalone programs whose business logic is not coupled to Kubernetes APIs. It does **not** mean Docker Compose is required.

Kubernetes is the target runtime for cloud deployment. Local Kubernetes using Kind, Minikube, or k3d is only a fast test harness for validating cloud-oriented Helm/Kustomize artifacts. Do not use Docker Compose as the primary local runtime.

Kubernetes provides DNS, service routing, scheduling, health management, secrets/config delivery, and autoscaling primitives. Aegis Go/Python code owns shard routing, worker ownership, membership interpretation, consistent-hash-ring construction, and owner redirect behavior.

---

# Global Development Constraints — Strictly Enforced

## 1. Platform-Agnostic Core
The core distributed system, especially the Go Control Plane and Python GPU Agent, must work standalone irrespective of Kubernetes. Core domain logic must not import Kubernetes packages or depend on Kubernetes APIs. Kubernetes is an external orchestration layer applied on top.

Kubernetes is still the target runtime for cloud deployment, and local Kubernetes using Kind/Minikube/k3d is only a fast validation harness for cloud-oriented artifacts. Kubernetes provides DNS, routing, scheduling, secrets/config delivery, health management, and autoscaling primitives; Aegis Go/Python code owns shard routing, worker ownership, membership interpretation, ring construction, failure detection, and owner redirects.

## 2. Versioning
Use recent stable package versions for all external artifacts and explicitly pin them. Do not use floating `latest` tags. Pin versions in Dockerfiles, Helm values, Kustomize overlays, CI, language package files, `.tool-versions`, and docs.

Use current stable versions where ecosystem-compatible, including Go, Python 3.13+, Redis, Kafka, OpenTelemetry Collector, LGTM stack, Kubernetes, Terraform, Helm/Kustomize, Kind, Tilt, PostgreSQL, and MinIO/S3-compatible tooling. Vector is not a required component in this architecture; OpenTelemetry Collector handles telemetry routing.

## 3. Git Workflow
Use atomic commits and semantic commit messages such as `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `refactor:`, and `perf:`. The development history must be structured, logical, and reviewable.

## 4. Human Idiosyncrasies
Reflect realistic human developer traits in code, Markdown docs, architecture notes, and articles. Leave occasional meaningful `// TODO:`, `# TODO:`, `// FIXME:`, or `# FIXME:` comments for credible complex edge cases such as transient network drops, future gRPC mTLS, retry tuning, Redis Pub/Sub hints, GPU vendor-specific collectors, or future Operator/CRD support.

Do not overuse TODO/FIXME comments and do not add fake-looking noise.

## 5. Monorepo Structure
Use a monorepo containing:

- Go Control Plane
- Python GPU Agent
- Python Postmortem Composer
- Go Sink/Fanout Workers
- protobuf definitions
- Kafka topic definitions
- cloud-targeted Kubernetes manifests
- Helm/Kustomize configuration
- optional Terraform code
- Devcontainer setup
- Tilt local Kubernetes inner-loop config
- observability config
- tests
- docs
- human-written system postmortem

## 6. Devcontainer Approach
Use VS Code Devcontainers. Create a primary `.devcontainer/` config. The Devcontainer is the reproducible developer workstation/control room, not the primary application runtime.

The repository may live on the developer filesystem, for example inside WSL, while VS Code opens it in a Devcontainer context. The Devcontainer contains pinned tools and CLIs: Go, Python, `kubectl`, Helm, Kustomize, Terraform, Kind, Tilt, protobuf/gRPC tools, linters, test tools, and useful service CLIs such as Kafka/PostgreSQL/MinIO clients.

Aegis services and infrastructure dependencies run in Kubernetes. The local Kubernetes validation cluster runs the Go CP, Python Agent, Kafka, Redis, PostgreSQL, MinIO, Composer, Sink Workers, OpenTelemetry Collector, and observability stack.

Use Tilt as the MVP Kubernetes inner-loop tool. Developers run `tilt up` from inside the Devcontainer. Tilt watches the mounted repository, applies Helm/Kustomize manifests, streams logs, sets port-forwards, and redeploys changed services into the local Kubernetes validation cluster.

Tilt must not blindly rebuild every container image on every source change:
- Python source changes should use Tilt live-update file sync plus process reload/restart where practical.
- Go source changes should use fast incremental rebuilds or live-update-assisted binary rebuilds, then restart only the affected workload.
- Dependency, Dockerfile, and base-image changes should trigger full image rebuilds.
- Helm/Kustomize changes should re-apply manifests.
- Stateful infrastructure such as Kafka, Redis, PostgreSQL, and MinIO should not restart for application source changes.

## 7. Workflow Tooling
Use strict linting, static typing, and test coverage:

- Go: `golangci-lint`, `go test`, `go test -cover`, race detector where useful.
- Python: `ruff`, `mypy`, `pytest`, `pytest-cov`.
- Kubernetes inner loop: `tilt`, `kubectl`, `kind`, Helm, Kustomize.
- CI: lint, type checks, unit tests, integration tests, E2E tests, image build validation, Kubernetes manifest validation.
- gRPC/protobuf: deterministic code generation.

## 8. Testing Structure
Use root-level test directories:

- `tests/unit/` for isolated unit tests.
- `tests/integration/` for standard integration tests that verify a small set of directly connected components, not the whole system.
- `tests/e2e/` for a few system-wide end-to-end tests covering critical Aegis workflows.
- `tests/chaos/` for failure injection, overload, failover, and performance-resilience tests.

Do not mislabel E2E tests as integration tests, and do not create an explosion of E2E tests for every subset of services.

## 9. Communication
Synchronous service-to-service control operations must use gRPC.

Asynchronous workflows, fanout, retries, postmortem generation events, and delivery pipelines must use Kafka. This intentionally refines the original “strictly gRPC” rule so Kafka can serve as the durable event backbone.

## 10. Telemetry
Every service must natively use OpenTelemetry to export traces and metrics through OpenTelemetry Collector. Propagate correlation IDs across gRPC, Kafka, Composer, sink workers, and owner redirects.

## 11. README Requirements
The README must start with one overarching Mermaid.js system design diagram combining Aegis application architecture and the cloud Kubernetes runtime. The diagram must have the clean look and feel of elite industry system-design diagrams: clear boundaries, few crossing lines, meaningful grouping, calm labels, and readable flow from detection to postmortem delivery.

The diagram must show Kubernetes as the orchestrator of standalone Aegis systems, but it must not list every technology used. Helm/Kustomize/Terraform/Tilt/devcontainer should be documented in the README but do not need to appear in the diagram unless they clarify runtime behavior.

The README must include:

- overview
- problem statement
- architecture summary
- cloud deployment notes
- local Kind/Minikube/k3d test notes
- service list
- Kafka topic list
- postmortem workflow
- sink fanout workflow
- observability instructions
- testing instructions
- KEDA scaling explanation
- Helm/Kustomize explanation
- optional Terraform explanation
- categorized **Technologies Used** inventory

All technologies, protocols, algorithms, and infrastructure components must be listed in the README technology inventory, not necessarily in the system design diagram.

---

# Section 1: Repository Initialization, Cloud Kubernetes Artifacts, and Documentation

## Role
Platform Engineer

## Tasks
1. Initialize the monorepo and Devcontainer setup.
2. Create cloud-targeted Helm/Kustomize artifacts with local overlays for Kind/Minikube/k3d testing.
3. Add Tilt configuration for the local Kubernetes inner loop. Tilt must run from inside the Devcontainer and must use live update/fast rebuild rules instead of full image rebuilds for every source change.
4. Document the three-layer development model: WSL/developer filesystem stores the repo, Devcontainer provides the toolchain/control room, and Kubernetes runs the actual Aegis workloads.
5. Deployable components: Go CP, Python GPU Agent, Postmortem Composer, Sink Workers, Kafka, Redis, PostgreSQL, MinIO/S3-compatible storage, mock Slack service for non-prod, OpenTelemetry Collector, observability stack, optional mock AI server.
6. Generate `README.md`, `agents.md`, and `claude.md` or equivalent AI-assistant instruction file, following the global README and tooling requirements.
7. README must begin with one AI-generated Mermaid.js system design diagram. Do not hardcode the diagram in this prompt.

The diagram instruction:
- create one overarching Mermaid.js diagram at the start of the README;
- use clean, elite system-design aesthetics: grouped boundaries, minimal clutter, readable labels, and few crossing lines;
- show Aegis application architecture and cloud Kubernetes as the orchestrator;
- show Kind/Minikube/k3d only as local validation, not the target runtime;
- show the core runtime path: Python GPU Agents, Go CP replicas, Redis membership/locks, Kafka backbone, Python Postmortem Composer, AI endpoint, Go Sink Workers, Slack/PostgreSQL/S3 destinations, OpenTelemetry Collector, observability stack, and KEDA/HPA scaling;
- do not include every technology in the diagram; Helm/Kustomize/Terraform/Tilt/devcontainer belong in the README technology inventory and workflow docs unless visually necessary;
- ensure the diagram is valid in Mermaid Live Editor.

8. `agents.md` must document the CP, Agent, Composer, Sink Workers, fallback behavior, retry behavior, failure boundaries, and CP discovery/owner redirect behavior.
9. `claude.md` must define repo context, coding rules, tests, architecture boundaries, commands, commit style, and documentation expectations.

---

# Section 2: System Workflow

## Role
Distributed Systems Architect

## Tasks
Implement this MVP workflow:

```text
Python GPU Agent -> gRPC telemetry/heartbeat -> Go CP -> deterministic failure detection -> TriggerDiagnostics -> Kafka diagnostic/postmortem request -> Postmortem Composer -> AI endpoint -> Kafka postmortem -> Go Sink Workers -> Slack/PostgreSQL/S3
```

The Go CP must not call Slack, PostgreSQL, S3, or the AI model server. It only handles ownership, telemetry ingestion, deterministic failure detection, diagnostics coordination, state transitions, Redis locks, deterministic incident IDs, and Kafka event publication.

The AI model is never used to decide whether a GPU/AI worker has failed. AI is used only after the CP has detected an incident and produced a diagnostic event.

Define protobuf/gRPC contracts for Agent heartbeat/telemetry streams, Agent diagnostic response, and the required CP-to-Agent `TriggerDiagnostics` command.

---

# Section 3: Go Control Plane — Sharding, State, Concurrency, Load Shedding

## Role
Senior Golang Engineer

## Tasks

## 1. CP Membership and Consistent Hashing
Each Go CP registers itself in Redis using a TTL membership lease, refreshes the lease periodically, and polls Redis for active CP members. Redis is **not** a hash-ring engine; it only stores CP membership, TTL leases, and locks.

Workers must not register themselves directly in Redis in the MVP. Workers reveal their `worker_id` when they connect/heartbeat. The CP ring is built from CP replicas only, and worker ownership is computed as `hash(worker_id) -> first clockwise CP virtual node`.

Each CP builds its own in-memory consistent hash ring from the Redis CP member list. The list must be sorted deterministically and converted into a membership fingerprint. If the fingerprint changes, rebuild and safely swap the ring using `atomic.Value`, atomic pointer, or `RWMutex`.

Use virtual nodes to reduce uneven worker distribution.

`# TODO:` CPs may persist compact `last_seen` worker snapshots in Redis for faster failover recovery, but workers still must not talk to Redis directly.
`# TODO:` Redis Pub/Sub or keyspace notifications may later provide faster ring-rebuild hints, but polling plus TTL expiry is the MVP source of truth.

## 2. Control-Plane Discovery and Owner Redirect
The Python Agent must not hardcode a CP replica. It starts with configured bootstrap URLs. In Kubernetes, this is typically a ClusterIP service such as `aegis-control-plane.aegis-system.svc.cluster.local:50051`; outside Kubernetes it may be any hostname/IP.

On connection, the Agent sends stable `worker_id`. The receiving CP checks ring ownership:
- if owner: accept stream;
- if not owner: return a normal network-address redirect hint, e.g. pod DNS/IP/hostname;
- if hint fails: Agent falls back to bootstrap URL and repeats discovery.

The Agent must not read Redis or build the ring. Kubernetes only routes to reachable endpoints; Aegis code decides ownership.

## 3. Incident State Machine
Implement observable/testable states:
`HEALTHY -> SUSPECTED -> DIAGNOSTICS_TRIGGERED -> DIAGNOSTICS_COLLECTED -> POSTMORTEM_REQUESTED -> POSTMORTEM_GENERATED -> DELIVERY_IN_PROGRESS -> DELIVERED/DELIVERY_FAILED -> RESOLVED`.

## 4. Failure Detection and Postmortem Event Emission
The Go CP must use deterministic rule-based failure detection in the MVP. The Python Agent streams heartbeat and GPU/model telemetry to the owning CP. The CP maintains rolling per-worker health state and evaluates rules using agent metrics.

MVP failure rules must include:
- missed heartbeat threshold;
- sustained high GPU temperature;
- sustained VRAM pressure;
- ECC error burst;
- model-server unhealthy status;
- sustained inference latency spike;
- explicit synthetic failure flag from the mock agent.

The CP must not use the AI model to decide whether a node has failed.

When a worker crosses a failure threshold, the CP must:
1. confirm it owns the `worker_id`;
2. transition the worker to `SUSPECTED`;
3. acquire Redis incident lock for `worker_id`;
4. generate deterministic `incident_id`;
5. send `TriggerDiagnostics` to the Agent if reachable;
6. publish a diagnostic/postmortem-request event to Kafka.

If diagnostics are collected, publish a complete diagnostic event. If the Agent is unreachable, publish a degraded event using the last known telemetry window and set `diagnostic_status` to `partial` or `unavailable`.

Suggested CP internal components:
- `TelemetryIngestor`: receives gRPC heartbeat/telemetry;
- `WorkerHealthStore`: keeps rolling state per `worker_id`;
- `FailureDetector`: applies deterministic rules;
- `IncidentManager`: handles state transitions, locks, and incident IDs;
- `DiagnosticsCoordinator`: sends `TriggerDiagnostics`;
- `KafkaPublisher`: emits diagnostic/postmortem request events.

## 5. Scalable Heartbeat Expiry Detection
The Go CP must not detect missed heartbeats by scanning every worker in a tight global loop. Use a shard-local heartbeat tracker for workers owned by the CP.

Required data object shape:

```go
type WorkerState struct {
    WorkerID        string
    LastHeartbeatAt time.Time
    DeadlineAt      time.Time
    State           WorkerHealthState
    Generation      uint64
}

type HeartbeatDeadline struct {
    WorkerID   string
    DeadlineAt time.Time
    Generation uint64
}
```

The CP should maintain a priority queue/min-heap ordered by `DeadlineAt`.

On heartbeat:
- update `LastHeartbeatAt`;
- compute new `DeadlineAt`;
- increment `Generation`;
- push a new `HeartbeatDeadline` into the heap.

Core expiry loop shape:

```go
for {
    item := heap.Peek()
    sleepUntil(item.DeadlineAt)
    item = heap.Pop()

    current := workers[item.WorkerID]
    if current.Generation != item.Generation {
        continue // stale deadline
    }
    if now.Before(current.DeadlineAt) {
        continue
    }
    handleMissedHeartbeat(item.WorkerID)
}
```

`handleMissedHeartbeat(workerID)` must confirm ownership, acquire Redis incident lock, generate deterministic `incident_id`, trigger diagnostics if possible, and publish a diagnostic/postmortem-request event. During shard ownership changes, newly owning CPs should use a takeover grace period before declaring newly acquired workers failed.

`# TODO:` Persist compact last-heartbeat snapshots in Redis for faster recovery after CP failover.

## 6. Deterministic Incident IDs
Every incident must have deterministic `incident_id` derived from `worker_id`, failure type, and failure window/timestamp bucket.

Redis locks prevent concurrent processing. Deterministic incident IDs provide stable incident identity across retries, CP failover, Kafka duplicate handling, sink idempotency, and archive paths. Kafka offsets/sequence numbers identify transport messages, not the business incident.

Use random UUIDs for `event_id` if needed, but do not use random UUIDs as the primary `incident_id` for detected worker failures.

## 7. Concurrency and Load Shedding
Route incoming telemetry/heartbeats into bounded Go channels consumed by worker pools. If the queue is full, use non-blocking send with strict timeout and return `codes.ResourceExhausted`.

## 8. Distributed Locking
If a worker misses the heartbeat threshold, acquire a Redis lock for `worker_id` before triggering diagnostics. Correctness during stale-ring windows must rely on Redis locks, deterministic incident IDs, idempotent Kafka publication, and Agent reconnect fallback.

## 9. Kafka Publication
Publish structured events to Kafka: `aegis.incident.detected`, `aegis.diagnostics.requested`, `aegis.diagnostics.collected`.

## 10. Tests
Unit tests: hashing, ring rebuild, membership fingerprint, safe ring swap, owner redirect, Redis leases, state transitions, load shedding, `ResourceExhausted`, incident IDs, event envelopes.
Integration tests: Agent → CP, bootstrap → wrong CP → redirect → owner CP, CP → Redis, CP → Kafka.

---

# Section 4: Python GPU Telemetry Agent

## Role
Senior Python Engineer

## Tasks
1. Represent a monitored GPU node, AI worker node, or model-server host. Do not call it a data-plane service.
2. Configure one or more bootstrap CP URLs.
3. Connect, identify with `worker_id`, handle owner redirects, reconnect to hinted owner, fallback to bootstrap on stale/unreachable owner, and use Full Jitter Exponential Backoff.
4. Do not connect to Redis or register worker presence directly in Redis. Only CPs use Redis.
5. If no real GPU exists, emit configurable mock metrics and simulate normal operation, VRAM pressure, overheating, latency spikes, missed heartbeat, freeze, ECC burst, model-server crash, network partition, and restart.
6. Agent telemetry must include enough fields for CP rule-based detection: `worker_id`, heartbeat timestamp, GPU utilization, VRAM used/total, temperature, power draw, ECC error count, inference latency, local queue depth, model-server health, and synthetic failure flag.
7. Maintain bounded diagnostic buffer with recent logs, telemetry, events, health state, and simulation state.
8. Implement `TriggerDiagnostics` response with structured JSON including `worker_id`, `incident_id`, timestamps, telemetry window, logs, failure indicators, GPU/model/agent health, and suspected cause hints.
9. Tests: mock metrics, failure simulation, backoff/jitter, redirects, bootstrap fallback, buffer limits, diagnostic schema.

---

# Section 5: Kafka Event Pipeline

## Role
Distributed Systems Engineer

## Tasks
1. Create topics: `aegis.incident.detected`, `aegis.diagnostics.requested`, `aegis.diagnostics.collected`, `aegis.postmortem.requested`, `aegis.postmortem.generated`, `aegis.postmortem.delivery.requested`, `aegis.postmortem.delivery.status`, `aegis.postmortem.delivery.retry`, `aegis.postmortem.delivery.dlq`.
2. MVP topic ownership:
   - Go CP publishes incident/diagnostic/postmortem-request events.
   - Python Postmortem Composer consumes diagnostics/postmortem requests and publishes generated postmortems.
   - Go Sink Workers consume generated postmortems and publish delivery status/retry/DLQ events.
3. Use a common envelope: `event_id`, `event_type`, `incident_id`, `worker_id`, `producer`, `timestamp`, `schema_version`, `correlation_id`, `causation_id`, `payload`.
4. Diagnostic event payload must include detection reason, failure type, severity, CP state, diagnostic status, recent telemetry window or diagnostic bundle, and optional engineer postmortem guidance.
5. Consumers must be idempotent; retries and DLQs must be explicit.
6. Tests: production, consumption, retry, DLQ, duplicate-event handling.

---

# Section 6: Postmortem Composer Service

## Role
AI Infrastructure Engineer / Distributed Systems Engineer

## Tasks
1. Consume `aegis.diagnostics.collected`.
2. Support optional engineer guidance: postmortem format, extra prompt, severity/audience/remediation context. If absent, generate a rich default postmortem.
3. Default sections: title, summary, severity, affected worker, timeline, detection, impact, suspected cause, contributing factors, recovery, remediation, follow-ups, trace links, raw appendix.
4. Call only an OpenAI-compatible endpoint such as vLLM/KServe/mock LLM. Treat it as black-box.
5. Validate Markdown output: required sections, preserved incident/worker IDs, non-empty content, metadata, minimal hallucinated infrastructure names.
6. Publish `aegis.postmortem.generated` with Markdown, metadata, model, prompt version, timestamp, validation status.
7. Tests: prompt construction, optional guidance, validation, mock LLM success/failure, malformed response, Kafka in/out.

---

# Section 7: Sink/Fanout Workers

## Role
Infrastructure Engineer

## Tasks
Use dedicated standalone Go Kafka consumer services for sink/fanout workers. They are Aegis-developed programs, not built-in Kafka broker features.

1. Implement Go-based Slack, PostgreSQL, and S3/MinIO sink workers.
2. Consume `aegis.postmortem.generated` or `aegis.postmortem.delivery.requested`.
3. Slack sends concise incident summary and full-postmortem reference.
4. PostgreSQL stores incident metadata and Markdown.
5. S3 stores immutable `postmortems/{worker_id}/{incident_id}/postmortem.md` plus metadata sidecar.
6. Publish delivery results to `aegis.postmortem.delivery.status`.
7. Failed deliveries retry, then move to `aegis.postmortem.delivery.dlq`.
8. Tests: delivery, persistence, archive, retry, DLQ, duplicate handling.

---

# Section 8: Observability

## Role
Observability Engineer

## Tasks
1. Emit OTel traces/metrics/structured logs from all services.
2. Propagate correlation IDs across gRPC, Kafka, Composer, sinks, and redirects.
3. Required metrics: active agents, missed heartbeats, incidents, diagnostics, postmortem latency, Kafka lag, delivery success/failure, DLQ count, CP queue depth, `ResourceExhausted`, redirect count, ring rebuild count, Redis polling errors, lock failures, AI latency/failures.
4. Dashboards: CP health, Agent health, Kafka pipeline, postmortem generation, sink delivery, ring membership, chaos/performance.
5. Tests must verify critical traces/metrics.

---

# Section 9: Integration Tests

## Role
Senior Test Engineer

## Tasks
Integration tests must verify closely related components across one subsystem boundary, not the entire Aegis system path.

Required integration tests:
1. Go CP + Redis: membership lease refresh, lock acquisition, TTL expiry handling.
2. Go CP internals: heartbeat tracker + failure detector + incident manager using deterministic worker states.
3. Go CP + Kafka producer: diagnostic/postmortem-request event publication with the common envelope.
4. Python Agent + Go CP gRPC: heartbeat/telemetry stream, owner redirect, and `TriggerDiagnostics` round-trip.
5. Postmortem Composer + mock LLM: prompt construction, validation, and generated postmortem output.
6. One Go Sink Worker + dependency at a time: Slack mock, PostgreSQL, or MinIO/S3 persistence.
7. Observability integration for one representative service path: trace/correlation ID propagation across one boundary.

Do not create many subset E2E tests disguised as integration tests. Output integration artifacts to `/reports/integration/`.

---
# Section 10: End-to-End Tests

## Role
Senior Test Engineer

## Tasks
E2E tests must be few, system-wide, and focused on critical Aegis behavior across Agent, CP, Kafka, Composer, Sink Workers, and dependencies.

Required E2E tests:
1. **Happy-path incident:** mock GPU failure -> CP detection -> diagnostics -> postmortem generation -> Slack/PostgreSQL/S3 delivery.
2. **Owner redirect and failover:** Agent connects through bootstrap, gets redirected to owner CP, then survives CP ownership change without duplicate incidents.
3. **Degraded diagnostics path:** Agent becomes unreachable after last telemetry; CP emits partial/unavailable diagnostic event and the Composer generates a postmortem that clearly marks diagnostics as degraded.

Keep E2E tests limited. Do not create separate E2E tests for every pair of services.

---
# Section 11: Chaos and Performance Tests

## Role
Staff Site Reliability Engineer

## Tasks
Store tests under `/tests/chaos/`. Keep them separate from integration and E2E tests.

Required system-wide chaos/performance tests:

1. **Vanishing Control Plane**
   - kill one CP replica;
   - assert Redis lease expiry;
   - assert membership fingerprint change;
   - assert in-memory ring rebuild;
   - assert ownership transfer;
   - assert affected Agents reconnect;
   - assert no duplicate postmortems.

2. **Thundering Herd and Backpressure**
   - simulate 100 failing/reconnecting Agents;
   - assert bounded CP queues;
   - assert `ResourceExhausted`;
   - assert jittered retries;
   - assert no redirect storm;
   - assert no uncontrolled duplicate Kafka events.

3. **Downstream Pipeline Degradation**
   - slow or fail Composer/sink workers;
   - assert Kafka lag and retry behavior are visible;
   - assert KEDA scaling metrics cross configured thresholds;
   - assert CP detection remains unaffected;
   - assert permanent delivery failures reach DLQ;
   - assert no postmortem is silently lost.

Output raw artifacts to `/reports/chaos/`: timeline, metrics, trace IDs, logs, Redis membership snapshots, ring rebuild events, Kafka lag, and recovery confirmation. These artifacts support the human-written system postmortem.

---
# Section 12: Human-Written Aegis System Postmortem

## Role
Staff Engineer / Technical Writer

## Tasks
Create `/docs/postmortems/aegis-system-postmortem.md`.

This is different from Aegis-generated GPU incident postmortems and must not be auto-generated by Aegis. Write it manually from integration, E2E, chaos, and performance artifacts. Include:

- project goal
- architecture
- experiment setup
- failures
- bottlenecks
- scaling limits
- Kafka backlog behavior
- Redis lease behavior
- CP failover
- ring rebuild behavior
- owner redirects
- degraded diagnostics behavior
- AI failure behavior
- sink failure behavior
- tradeoffs
- future improvements

---
# Section 13: Cloud Kubernetes, Helm/Kustomize, Terraform, and KEDA

## Role
Cloud Native Engineer

## Tasks
1. Build Helm/Kustomize artifacts for cloud Kubernetes deployment. Local Kind/Minikube/k3d validates the same cloud-style manifests with small overlays only where unavoidable.
2. Use Tilt for local Kubernetes inner-loop automation only; cloud deployment remains Helm/Kustomize-driven.
3. Deploy CP, Agent, Composer, Sink Workers, Kafka, Redis, PostgreSQL, MinIO/S3-compatible store, OTel Collector, observability stack, mock Slack, and optional mock AI server.
4. Terraform is optional and may provision cloud or local Kubernetes foundations, namespaces, storage classes, and Helm releases. Aegis must still deploy to an existing cluster without Terraform.
5. Use KEDA:
   - CP: active agents and CP queue depth as primary MVP scalers; CPU/memory as fallback.
   - Composer: Kafka lag as primary MVP scaler; CPU/memory as fallback.
   - AI server if local: concurrency/latency/CPU/memory.
   - Sink workers: Kafka lag and retry depth as primary MVP scalers; delivery latency as an observed metric.
6. Python Agent should run as a DaemonSet for node-level monitoring in both cloud and local Kubernetes validation. For synthetic high-cardinality worker simulation, add a separate simulator Deployment rather than changing the node-agent deployment model.
7. When CP replicas scale, Redis membership changes and CPs rebuild in-memory rings via polling/fingerprint mechanism.
8. Document how Kubernetes-provided URLs enter config and how the same services work with non-Kubernetes hostnames/IPs.

---
# Section 14: Security and Production Hardening Notes

## Role
Security-Conscious Infrastructure Engineer

## Tasks
Implement expected security configurations without overbuilding the MVP:

- gRPC mTLS as a documented and optionally wired production mode;
- Kafka authentication and ACL notes;
- Redis authentication;
- PostgreSQL and S3 credentials through Kubernetes Secrets;
- Slack webhook secret handling through Kubernetes Secrets;
- basic Kubernetes NetworkPolicy between major components;
- least-privilege Kubernetes service accounts;
- basic rate limiting/backpressure configuration;
- secret rotation notes.

Do not add overkill supply-chain or policy frameworks such as image signing, SBOM pipelines, or admission-policy systems in the MVP.

---

# Section 15: Final Deliverables

1. Monorepo source.
2. Cloud-targeted Helm/Kustomize artifacts plus local Kubernetes validation overlay.
3. VS Code Devcontainer with Tilt/kubectl/Helm/Kustomize/Kind tooling and documented WSL/devcontainer/Kubernetes development model.
4. Go Control Plane.
5. Python GPU Telemetry Agent.
6. Kafka pipeline.
7. Redis lease/membership/lock logic.
8. In-memory consistent hash ring built from CP membership only.
9. Owner redirect behavior and worker-no-Redis rule.
10. Postmortem Composer.
11. Mock or real AI endpoint integration.
12. Slack/PostgreSQL/S3 sink workers.
13. OpenTelemetry instrumentation and Collector config.
14. Observability dashboards.
15. Unit, integration, E2E, and chaos/performance tests.
16. KEDA scaling config and Tilt local inner-loop config.
17. Optional Terraform.
18. README instruction fulfilled with one AI-generated overarching Mermaid system diagram.
19. Categorized README technology inventory.
20. Human-written Aegis system postmortem.
21. Atomic semantic commit history.

---
# Section 16: Commit Discipline

The exact commit sequence should follow implementation order, but commits must remain atomic, semantic, and reviewable.

Rules:
- one logical change per commit
- use semantic prefixes such as `feat:`, `fix:`, `docs:`, `test:`, `chore:`, `refactor:`, `perf:`
- commit generated docs, tests, and infra changes separately when useful
- avoid giant mixed commits
- avoid single-dump histories
