
FUTURE TODO

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



CLASSIFY
===
- use New<StructName>() pattern throughout, avoid direct struct instantiation as much as possible
- Pass explicit "Key" of grabbing workerID. Because some kafka client does have a workerID they simply pass nonWorkerID value to it. Bad pattern
- parameterize eventTypes and topics 
- obsv to track corrupt FSM count and SRE CLI to inform the dev of possible causes and how it should not be taken seriously
- remove hardcoded text and literals in tests e.g. "Node failure suspected due to missed heartbeat min-heap expiry"
- fix watchdog log/msg duplication across CP instances/pods (solve via Redis leader election lease or sharding)
- fix TopicCorruptFSMDLQ generation duplication in watchdog for a single instance (Watchdog repeatedly publishes the same stuck incident to DLQ because it never modifies/deletes the Redis state). Solution: advance the FSM state to a terminal STUCK_FAILED state, or write an "alerted" marker to Redis to skip subsequent loops.
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
- refactor sink service to use two in-process Kafka consumer instances with separate group IDs (e.g., `aegis-sink-file` and `aegis-sink-email`) instead of a composite program loop over sink adapters. Weakness of current program loop: a failure or timeout in an external dependency (like SMTP email) prevents clean offset commitment, causing duplicate file sink writes on retry and coupling independent destinations into a single failure domain.
- ensure just enough info is sent between services(e.g. agent sends object that includes old telemetry and logs) to save bandwidth
- refactor `readExpectedEvents` in `server_integration_test.go` to scan expected topics concurrently instead of sequentially to drastically speed up CP integration tests.
- parameterize the loop lag interval in control-plane `membershipLoop` so integration tests can run quickly without delayed first loop runs, replacing the current pre-seeding test hack.
- hint in readme that OLLAMA_MODEL docker arg can be changed to cache a model in a dockerlayer and if possible provide more friendly means to change it than going to docker file
- strip out agent "backoff" check
- synth postmortem
- priority queue tests(intg and unit)
- priority queue with varying agent timeout
- wipe-test-state can be renamed to wipe-infra-state to make it more general for test and non tests cases
- WipeTestState kafka json file cache, ensure it is discarded when topics.yaml is edited
- tmp folder usage scoped
- ensure intg log prints are properly ordered
- remove sink http test skip()
- confirm WipeTestState method call passes the right args across all intg tests
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
- observability stack(e.g. aegis_fsm_illegal_transitions_total metrics)
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
- ensure cluster components authorization e.g. the controlplane current does not have a verification process for a worker sending it telemetry
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