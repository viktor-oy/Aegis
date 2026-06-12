
FUTURE TODO
- ensure local infra starts and works
- CORRUPT_FSM log: print all important details
- confirm retry, dlq mechanics
- validate cp.windows[workerID] objects, it does not seem to be used yet and it keep adding objects which can lead to memory leak
- todo: postmortem gen thunderingherd due to multiple related GPU(can be virtual) failures. 
Normally agent running as daemonset signifies GPU failure events will not be correlated, but this is not true 
considering the underlying GPU to node configurations e.g. The "MIG" (Multi-Instance GPU) Scenario OR multiple 
GPUs sharing the same cooling fan. Also multiple agents being unreachable at once maybe due to undelying k8s issue, 
but aegis should be k8s agnostic
- postmortem coalescing maybe by virtual and physical GPU ID and node ID, multiple errorType of same worker, or more...
- Telemetry Snapshot (The Lead-Up)
A brief table or summary of the metrics exactly before the crash. Elite teams don't just want to know it crashed; they want to see the slope.
- ensure sections a pre-fix postmortem should contain according to industry standard used by elite teams, is followed
- add GPU brand/model agnostic interface in the code and test implemented brands/model on real GPUs. Start with free and cheap GPU you can access.
- ascertain if some GPU failure types should kill/cordon the OS(pod in k8s) the agent is running in, although already the OS might be configured to kill/cordon itself if the GPU enters certain error states
- agent(Captures Dump/stacktrace -> Sanitizes&removes sensitive confidential details -> Summarizes) -> CP -> composer receives dumps/stacktrace of GPU, adds a summarized version to prompt, then append the dump/stacktrace at at the end(not in the prompt) of the AI-gen postmortem
- use any of kafka topic namespacing, separate kind cluster for test, test containers. And remove Payload Filtering and other unnecessary constructs
- scenario runner advanced stages: Redis Split-Brain / CP Partitioning (test lease expiry) and Kafka Broker Drops (test producer retries)


ORGANIZE
===
- prefix hashring DB lease keys with "aegis:" and exclude them from wipe_infra_state
- refactor sink service to use two in-process Kafka consumer instances with separate group IDs (e.g., `aegis-sink-file` and `aegis-sink-email`) instead of a composite program loop over sink adapters. Weakness of current program loop: a failure or timeout in an external dependency (like SMTP email) prevents clean offset commitment, causing duplicate file sink writes on retry and coupling independent destinations into a single failure domain.
- CRITICAL: monotonic clock and safety with telemetry timestamp(used by priority queue) provided by agents

- poor quality code: tracker saves an array of workers and also has a priority queue containing worker info. This is too stateful and could contain stale worker data because k8s could restart worker(deamonsets) with a different podname at anytime and the tracker will not know. This can cause unnecessary missed heartbeat events/fsm for deliberately(manually or by k8s) deleted pods. Hashring is safe from this, because it does not store workerID, it only stores CP data and it constantly rebuilds the hashring with redis leases refreshed by CP. Redirect could be watched to purge those states but that might not be enough

CLASSIFY
===
- tombstone feature in composer and sink to avoid those services processing staleevents and sending obsolete notification. But without concepts(e.g. FSM) from CP bleeding into those services i.e. they should know little and avoid being too stateful.

- [ ] infra: Adjust `/infra` and other workflow code (e.g. Helm values, python scripts) for `AEGIS_TOPIC_POSTMORTEM_GENERATED`, `AEGIS_TOPIC_DELIVERY_STATUS`, and `AEGIS_TOPIC_DELIVERY_DLQ` environment variables.

- Make the strict FSM feature optional
- should go "-race" flag be used?
- make FSM optional and switch to a distributed CP store entirely
- use waitGroup for control loops and other important areas
- ensure kafka consumers connect in an infinite loop OR ensure liveness check return err if consumer lost connection with broker. WHich ever is better
- CRITICAL: use redis unlink/scan instead of DEL/KEYS. But recall the former is unusable when synchronous exec flow is required to avoid race conditions
- replace dlq with fsm: "aegis:cp:dlq:corrupt:*"
- (continue_on_err: true) is questionable in some scenario yaml areas
- Feature: Resolved/Healthy fsm marker GC. Maybe watchdog scan can handle(with acquireFSMLock) this. Note that FSM corrupt marker does not need to GC because it will be deleted by aegiscli, just ensure --force(without --fix-corrupt-fsm) will fail if there is FSM corrupt marker
- FSM chain of events/trail and report to aegis devs
- protect various interval timers(e.g. for various loops in main.go) from compute delay to ensure they are steady

- format ageiscli output properly
- explicitly add `failure_type` into Composer's `aegis.postmortem.generated` payload, and fix the `causation_id` UUID mapping to bypass expensive `ListActiveWorkerStates` fallback.
- sometimes log shows no corruption exists and stuck heartbeat keeps showing despite services/infra being up for the FSM to progress
- investigate missing `current_state` and `incident_id` fields in watchdog stuck incident log output. IMPORTANT FOR scenario runner log observability UX
- start server in various env(including prod) mode, watch stability and runner scenario runner(need to receive env=scenario param) against them
- why is cli --force not asking y/n. Ensure tested
- let aegiscli take a --fix-all param, which should present a text prompt for better Disaster Mgmt instead of y/n . Ensure tested
- keepalives where necessary?
- heartbeat tests
- can infrastart logic be modularized across runner.py and ensure_test_infra.py?
- "Auto-Recovered Metadata"
- log effective(pass param or fallback(if missing or err)) env param across aegis services
- getWorkerForCP Memoization
- remember reasoning: test integrity that test containers
- TestIntegration_Incident_CorruptFSM_WatchdogAlert_And_Sharding and TestIntegration_Incident_FSMDeferredEventTimeout have some level of parity and can be kept DRY
- alternative(especially because redisTTL still introduces determinism even if the value is large and lag forgiving) to redis approach of enforcing FSM chronological determinism: consolidate Kafka topics into a generalized firehose event topic to strictly enforce FSM chronological determinism via single-partition routing.
  - NOTE: The Redis approach is vulnerable to Polling Blindness (a Nyquist theorem violation) where the Watchdog can sleep through the expiration of a deferred event. The firehose architecture prevents this entirely.
- is it possible for intg kustomize overlay(which includes values yaml) to inherit local, in order to keep the code DRY?

- add to ensure_test_infra, logic that checks if kafka and redis has grown enormously due to previous tests and then nukes it
- Resolve Kafka-Go Deadlocks & Enrich Health Checks: The `aegis-sink` and CP can hang silently because `kafka-go`'s default `ReadMessage` blocks indefinitely without TCP KeepAlives enabled. When Tilt or K8s rebuilds Kafka, the Kubernetes networking layer drops the connection silently, leaving consumers holding a half-open "dead" TCP socket. Solution: 1) Inject a `kafka.Dialer` with `Timeout: 10s` and `KeepAlive: 30s` into the `ReaderConfig` to force the OS to drop dead connections. 2) Update `infra/helm/aegis/templates/apps.yaml` to ensure all four Aegis services (agent, composer, cp, sink) have native `livenessProbe` and `readinessProbe` definitions.
- is the SRE-CLI tool its args and arg combination complex(is readme examples)? Even the conditions look complex
- combine log print in array loop into one log if possible for better log clarity e.g. watchdog loop could combine multiple errType checks into one log
- should sink delivery.Worker.seen map be replaced with redis?
- NOTE: SRE-CLI fixing an FSM that has unprocessed kafka msg will cause that msg to become orphaned stray msg. A similar event(from agent) can trigger an identical kafka msg(workerID+errType), hence two msgs exists increasing the likelihood of FSM corruption. This should be tackled(maybe FSM idempotence)
- implement TriggerDiagnostics in agent and let CP actually call it
- move main to /cmd
- fsm(including matrix) versioning for backward compactibility with active FSM after aegis is upgraded
- track.Observe need some cleanup, it always set WorkerHealthy while the agent might even be in a bad state
- should TestIntegration_Incident_KafkaPublishFailureRollback cleanup grpc stream or does ctx handle it already?
- INTG TEST SPEED: in aegis services(especially CP), across intg test files, aegis cluster(i.e. intg direct run, not in k8s infra) can be started once. A testfile should not shut it down, other test files can check if it is already running and just use it.
- ensure `--fix-corrupt-fsm` in SRE CLI checks for evidence of corruption (e.g., DLQ marker presence) before allowing the reset.
- use New<StructName>() pattern throughout, avoid direct struct instantiation as much as possible
- Pass explicit "Key" of grabbing workerID. Because some kafka client does have a workerID they simply pass nonWorkerID value to it. Bad pattern
- parameterize eventTypes and topics 
- obsv to track corrupt FSM count and SRE CLI to inform the dev of possible causes and how it should not be taken seriously
- remove hardcoded text and literals in tests e.g. "Node failure suspected due to missed heartbeat min-heap expiry"
- should watchdog keep publishing kafka msg?
- fix TopicCorruptFSMDLQ generation duplication in watchdog for a single instance (Watchdog repeatedly publishes the same stuck incident to DLQ because it never modifies/deletes the Redis state). Solution: advance the FSM state to a terminal STUCK_FAILED(the FSM redis marker does not use TTL, this state gives it a timeout semantics) state, or write an "alerted" marker to Redis to skip subsequent loops.
  > [!IMPORTANT]
  > **Impl Plan Summary for STUCK_FAILED & DLQ Replay:**
  > 1. Introduce `state.WorkerStuckFailed` ("STUCK_FAILED") constant.
  > 2. Update `manager.go` `ValidTransition` to allow any active intermediate state to transition to `STUCK_FAILED`, and allow `STUCK_FAILED` -> `RESOLVED`.
  > 3. Update `ValidTransition` to explicitly allow `WorkerDeliveryFailed` -> `WorkerDelivered` to support SRE DLQ event replays without corrupting the FSM.
  > 4. In `watchdog.go` `InspectOnce`, when an incident exceeds the threshold, actively transition the FSM state in Redis to `STUCK_FAILED`.
- remove composer unit tests error that exists in multiple commits
- fix(sink,agent): enforce file/email sink idempotency and uuidv4 correlation IDs
- ensure the right TTL is set on the distributed lock
- self._attempt = 0
- redis scaling e.g. sentinel
- ensure all kafka topics are parameterized
- implement errorType preemption (partition Kafka by worker_id; make Preemption Matrix an explicit constant; if CP starts getting heartbeats again only heartbeat error can be preempted; fatal errors preempt warning states)
- O(1) defeated by O(N) search (e.g. for generation validation)
- integrate lint and tests into dev commit/push workflow
- ensure silent errors are not causing k8s restarts e.g. kafka seems to restart often
- analyze the effect of timeouts on all distributed lock usage. It maybe a watchdog is needed, also ensure the system is still protected if watchdog malfunctions(e.g. too late teimout refresh or failure)
- ensure just enough info is sent between services(e.g. agent sends object that includes old telemetry and logs) to save bandwidth
- refactor `readExpectedEvents` in `server_integration_test.go` to scan expected topics concurrently instead of sequentially to drastically speed up CP integration tests.
- hint in readme that OLLAMA_MODEL docker arg can be changed to cache a model in a dockerlayer and if possible provide more friendly means to change it than going to docker file
- strip out agent "backoff" check
- synth postmortem
- priority queue tests(intg and unit), with varying agent timeout
- wipe-test-state can be renamed to wipe-infra-state to make it more general for test and non tests cases
- WipeTestState kafka json file cache, ensure it is discarded when topics.yaml is edited

### Infrastructure
- [ ] **Unified Infrastructure Polling (`ensure_infra.py`)**: Consolidate `ensure_test_infra.py` and `runner.py` polling logic into a single shared script. The script should use a hybrid approach: querying the Tilt API (`http://localhost:<tilt-port>/api/view`) to ensure Kubernetes workloads are fully built and `Ready`, AND using `check_port()` to guarantee that local `kubectl port-forward` sockets are actually open and accepting connections (since Tilt API only confirms the port-forward bash script is running, not that the port successfully bound).
- [ ] Wire production gRPC mTLS once certificate issuance is decided.
- tmp folder usage scoped
- ensure intg log prints are properly ordered
- remove sink http test skip()
- confirm WipeTestState method call passes the right args across all intg tests for perf reason or better still confirm if the method call is needed at all for each test function
- Readme: common issues and troubleshooting guide. Also arrange rough-board
- allow complete prompt to be specified as a env arg in composer, if not fall back to current composer prompt generation
- ensure synthetic collector generated event follows a contract that can be met by various gpu stats tools
- seems CP does not create a complete kafka envelope, fix and ensure(in intg and unit tests also) the composer sends all data to composer
- also ensure contract between all kafka producer and consumer, also have types file shared between services and tests files, and remove redundant types
- across services, move service files imported into blackbox binary based intg tests into common folder
- rename triggerdiagnostics
- CP grpc refelction only in debug
- CP ensure submitdiagnosis throws unimplemented, including in agent
- ensure all services have health check
- ensure validtransition() and other validate methods work correctly and are used
- ensure diagnostics data generated by synthetic source is realistic
- confirm detection rules
- send physical and virtual GPU ID
- add node ID(important incase pod is rescheduled which means worker could be lost making workerID useless) and CP ID to postmortem
- reflect new IDs in log msgs
- ensure testing endpoints across services are only active in debug mode
- use binary blackbox approach for composer, makefile might need to be modified to include main package
- if real GPU collector has not yet been tested, then stash it
- remove tiltinfra/makefile run from all intg test, they should be executed before all test runs 

- ensure aegis works with real email server
http endpoints in readme
ensure make cmds only works locally to protect users
rename /artifacts
ensure cmd and instructions are platform agnostic
- add to todo: ensure all intg tests using binary blackbox approach or load unparameterized boostrap code
- todo: use Otel
- todo: ensure proper use of corrID generated by gpu collector
- avoid hardcoding config fields in intg e.g. kafka topics
- all kubectl cmd should(maybe optional for tilefiles due to allow_contexts method call ) explicitly specify context for disaster prevention
- improve scnerio runner with observability stack and query engines
===



- ensure require env or get env are used at the right places project wide
- ensure integration tests starts service as a subprocess, for production process parity.
- use PVC or similar(e.g. s3) means to load AI model instead of docker build or entryscript(to avoid data logistics or always redownloading)
- implement submitdiagnosis
- remove hardcoded configs including fallbacks(.urls ) that makes the program too forgiving instead of failing fast when the user has not provided config
- troubleshooting cap
- stop CP if any critical goroutine(e.g. membershipLoop) ends?
- is heap not enough to store worker state?
- cant UpdateRing currentRing locking be replaced with channels
- test coverage tool
- use helmCharts field
- remove tiltinfra run from all intg test to remove complexity, it should be started manually before running the test.
- also ensure tests code(especially init code) are DRY
- ensure all intg tests start the binary once
- ensure intg tests confirm that other events aegis does not react to, are emitted
- set llm:securityContext:runAsNonRoot: true and eliminate perm issues e.g. create user and create folder(./ollama) in dockerfile
- adjusts kubelet stream timeout and remove reconnect scripts
- improve quality of all health check services
- see if its better to replace kafka topic namespacing with test containers.
