//go:build integration

package incident_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	aegisv1 "github.com/aegis/aegis/gen/go/aegis/v1"
	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
	"github.com/aegis/aegis/services/control-plane/internal/testutil"
	"github.com/aegis/aegis/tests/integration/testutils"
	segmentiokafka "github.com/segmentio/kafka-go"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	incidentFactory *testutil.CPTestClusterFactory
	globalClients   []aegisv1.ControlPlaneTelemetryClient
)

func TestMain(m *testing.M) {
	testTopicPrefix := fmt.Sprintf("test-run-%d-", time.Now().UnixNano())
	testTopicPostmortemGenerated := testTopicPrefix + "postmortem.generated"
	testTopicDeliveryStatus := testTopicPrefix + "postmortem.delivery.status"
	testTopicDeliveryDLQ := testTopicPrefix + "postmortem.delivery.dlq"

	incident.TopicPostmortemGenerated = testTopicPostmortemGenerated
	incident.TopicDeliveryStatus = testTopicDeliveryStatus
	incident.TopicDeliveryDLQ = testTopicDeliveryDLQ

	extraEnv := map[string]string{
		"AEGIS_TOPIC_POSTMORTEM_GENERATED": testTopicPostmortemGenerated,
		"AEGIS_TOPIC_DELIVERY_STATUS":      testTopicDeliveryStatus,
		"AEGIS_TOPIC_DELIVERY_DLQ":         testTopicDeliveryDLQ,
		"AEGIS_FSM_WATCHDOG_INTERVAL_SECONDS": "1",
		"AEGIS_GRPC_MAX_MSG_SIZE":             "10485760", // 10MB to test oversized Kafka payloads safely
	}

	incidentFactory = testutil.NewCPTestClusterFactory(
		[]string{"cp-a", "cp-b"},
		testutils.DefaultEtcdUrls,
		testutils.DefaultKafkaBrokers,
		extraEnv,
	)

	// Start the cluster once.
	globalClients = incidentFactory.StartCluster(nil)

	// Ensure only our dynamically namespaced Kafka topics are created before tests run.
	// Standard topics are assumed to be created by `ensure_test_infra.py` or `make init-kafka`.
	topics := []string{
		incident.TopicPostmortemGenerated, incident.TopicDeliveryStatus, incident.TopicDeliveryDLQ,
	}
	testutils.EnsureKafkaTopics(nil, topics, 3)
	time.Sleep(2 * time.Second) // wait for topics to propagate

	code := m.Run()

	incidentFactory.Teardown()
	os.Exit(code)
}

func getWorkerState(ctx context.Context, eClient *clientv3.Client, workerID, errorType string) (*membership.WorkerState, error) {
	key := fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errorType)
	resp, err := eClient.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("not found")
	}
	var ws membership.WorkerState
	if err := json.Unmarshal(resp.Kvs[0].Value, &ws); err != nil {
		return nil, err
	}
	return &ws, nil
}

// waitForDLQEvent spawns Kafka readers on all partitions of the given topic
// and returns a channel that will emit the parsed EventEnvelope when the matcher function returns true.
func waitForDLQEvent(topic string, matcher func(state.EventEnvelope) bool) <-chan state.EventEnvelope {
	numPartitions := testutils.GetTopicPartitionCount(topic)
	foundCh := make(chan state.EventEnvelope, 1)

	for p := 0; p < numPartitions; p++ {
		go func(partition int) {
			reader := segmentiokafka.NewReader(segmentiokafka.ReaderConfig{
				Brokers:   testutils.DefaultKafkaBrokers,
				Topic:     topic,
				Partition: partition,
			})
			_ = reader.SetOffset(segmentiokafka.LastOffset)

			// We only need to wait a reasonable time for the test to complete
			rCtx, rCancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer rCancel()
			defer reader.Close()

			for {
				m, err := reader.ReadMessage(rCtx)
				if err != nil {
					break // timeout or closed
				}
				var env state.EventEnvelope
				if err := json.Unmarshal(m.Value, &env); err == nil {
					if matcher(env) {
						select {
						case foundCh <- env:
						default:
						}
						break
					}
				}
			}
		}(p)
	}

	return foundCh
}

func publishRawMessage(t *testing.T, topic string, payload interface{}) {
	t.Helper()
	writer := &segmentiokafka.Writer{
		Addr:     segmentiokafka.TCP(testutils.DefaultKafkaBrokers...),
		Topic:    topic,
		Balancer: &segmentiokafka.LeastBytes{},
	}
	defer writer.Close()

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal raw payload: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := writer.WriteMessages(ctx, segmentiokafka.Message{Value: data}); err != nil {
		t.Fatalf("failed to write raw message to %s: %v", topic, err)
	}
}

func TestIntegration_Incident_FSMTransitions(t *testing.T) {
	testutils.WipeTestState(t, "etcd")

	client := globalClients[0]

	eClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{testutils.DefaultEtcdUrls},
		DialTimeout: 5 * time.Second,
	})
	defer eClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	incidentID := ""

	// 1. Submit Telemetry to trigger detection
	stream, err := client.StreamTelemetry(ctx)
	if err != nil {
		t.Fatalf("failed to open telemetry stream: %v", err)
	}

	err = stream.Send(&aegisv1.AgentTelemetry{
		WorkerId:         workerID,
		Timestamp:        timestamppb.Now().AsTime().Format(time.RFC3339Nano),
		SyntheticFailure: "synthetic",
		CorrelationId:    "corr-" + fmt.Sprint(time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("failed to send telemetry: %v", err)
	}

	directive, err := stream.Recv()
	if err != nil {
		t.Fatalf("failed to receive directive: %v", err)
	}
	if directive.DirectiveType != "accepted" {
		t.Fatalf("expected accepted directive, got %s", directive.DirectiveType)
	}
	_ = stream.CloseSend()

	// 2. Wait for POSTMORTEM_REQUESTED state in etcd
	var workerState *membership.WorkerState
	found := false
	for i := 0; i < 50; i++ {
		workerState, _ = getWorkerState(ctx, eClient, workerID, string(state.FailureSynthetic))
		if workerState != nil && workerState.CurrentState == string(state.WorkerPostmortemRequested) {
			found = true
			incidentID = workerState.IncidentID // Capture deterministic ID
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !found {
		t.Fatalf("expected state POSTMORTEM_REQUESTED in etcd, got %v", workerState)
	}

	// Helper to push mock event and wait for etcd transition
	assertTransition := func(topic string, eventType string, newState state.WorkerHealthState) {
		if topic == incident.TopicDeliveryStatus {
			payload := map[string]interface{}{
				"incident_id":    incidentID,
				"sink":           "file",
				"status":         "delivered",
				"correlation_id": "corr-" + fmt.Sprint(time.Now().UnixNano()),
			}
			publishRawMessage(t, topic, payload)
		} else {
			env := state.EventEnvelope{
				EventID:       fmt.Sprintf("evt-%d", time.Now().UnixNano()),
				EventType:     eventType,
				IncidentID:    incidentID,
				WorkerID:      workerID,
				Timestamp:     time.Now().UTC(),
				CorrelationID: "corr-" + fmt.Sprint(time.Now().UnixNano()),
				Payload: map[string]interface{}{
					"state":  string(newState),
					"status": "delivered",
				},
			}
			publishRawMessage(t, topic, env)
		}

		found = false
		for i := 0; i < 150; i++ {
			workerState, _ = getWorkerState(ctx, eClient, workerID, string(state.FailureSynthetic))
			if workerState != nil && workerState.CurrentState == string(newState) {
				found = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !found {
			t.Fatalf("expected state %s after mock event %s, got %v", newState, eventType, workerState)
		}
	}

	// 3. FSM step: Composer generates postmortem -> CP immediately transitions to DELIVERY_IN_PROGRESS
	assertTransition(incident.TopicPostmortemGenerated, incident.TopicPostmortemGenerated, state.WorkerDeliveryInProgress)

	assertTransition(incident.TopicDeliveryStatus, "aegis.sink.delivered", state.WorkerDelivered)

	testutils.LogInfo(t, "✅ Integration test passed: FSM transitions verified against live etcd & Kafka")
}

func TestIntegration_Incident_StuckFSM_WatchdogAlert(t *testing.T) {
	testutils.WipeTestState(t, "etcd")

	// client not directly used, but server is running

	eClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{testutils.DefaultEtcdUrls},
		DialTimeout: 5 * time.Second,
	})
	defer eClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	incidentID := fmt.Sprintf("inc-%d", time.Now().UnixNano())
	errType := state.FailureSynthetic

	testutils.LogInfo(t, "📖 Starting Kafka readers for DLQ topic...")
	foundCh := waitForDLQEvent(incident.TopicCorruptFSMDLQ, func(env state.EventEnvelope) bool {
		return env.WorkerID == workerID && env.EventType == "aegis.cp.fsm_stuck_incident"
	})

	// Give the readers a moment to connect and set offset
	time.Sleep(1 * time.Second)

	// Fake an old state in etcd to trigger stuck metric
	oldTime := time.Now().UTC().Add(-20 * time.Minute)
	_, err := eClient.Put(ctx, fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errType),
		fmt.Sprintf(`{"worker_id":"%s","error_type":"%s","current_state":"%s","incident_id":"%s","correlation_id":"%s","updated_at":"%s"}`,
			workerID, errType, state.WorkerDiagnosticsTriggered, incidentID, "corr-"+fmt.Sprint(time.Now().UnixNano()), oldTime.Format(time.RFC3339Nano)),
	)
	if err != nil {
		t.Fatalf("Failed to manipulate etcd state for watchdog: %v", err)
	}

	testutils.LogInfo(t, "📖 Verifying Watchdog published stuck incident to DLQ topic...")

	foundStuckIncident := false
	select {
	case <-foundCh:
		foundStuckIncident = true
	case <-time.After(30 * time.Second):
		foundStuckIncident = false
	}

	if !foundStuckIncident {
		t.Fatalf("Watchdog failed to publish stuck incident metric to DLQ in integration test")
	}
	testutils.LogInfo(t, "✅ Integration test passed: Watchdog behavior verified against live etcd & Kafka")
}

func TestIntegration_Incident_CorruptFSM_WatchdogAlert_And_Sharding(t *testing.T) {
	testutils.WipeTestState(t, "etcd")

	eClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{testutils.DefaultEtcdUrls},
		DialTimeout: 5 * time.Second,
	})
	defer eClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	expectedProducer := state.CreateProducerRef("cp-a")
	errType := string(state.FailureSynthetic)

	testutils.LogInfo(t, "📖 Starting Kafka readers for DLQ topic...")
	foundCh := waitForDLQEvent(incident.TopicCorruptFSMDLQ, func(env state.EventEnvelope) bool {
		return (env.WorkerID == workerID || strings.Contains(env.CausationID, workerID)) && env.EventType == "aegis.cp.fsm_corrupt_marker"
	})

	time.Sleep(2 * time.Second)

	incidentID := "inc_poison_pill_" + fmt.Sprint(time.Now().UnixNano())
	_, err := eClient.Put(ctx, fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errType),
		fmt.Sprintf(`{"worker_id":"%s","error_type":"%s","current_state":"%s","incident_id":"%s","correlation_id":"%s","updated_at":"%s"}`,
			workerID, errType, state.WorkerSuspected, incidentID, "corr-"+fmt.Sprint(time.Now().UnixNano()), time.Now().UTC().Format(time.RFC3339Nano)),
	)
	if err != nil {
		t.Fatalf("Failed to seed etcd state: %v", err)
	}

	// Give the FSMConsumer in the CP processes enough time to join the Kafka consumer group and subscribe.
	time.Sleep(10 * time.Second)

	testutils.LogInfo(t, "📖 Publishing poison pill to trigger FSM corruption...")

	poisonPill := state.EventEnvelope{
		EventType:  incident.TopicPostmortemGenerated,
		WorkerID:   workerID,
		IncidentID: incidentID,
		Payload: map[string]any{
			"worker_id":    workerID,
			"failure_type": errType,
		},
	}
	publishRawMessage(t, incident.TopicPostmortemGenerated, poisonPill)

	testutils.LogInfo(t, "📖 Verifying DLQ marker was created in etcd and DLQ event was published...")

	foundCorruptIncident := false
	var actualProducer string
	select {
	case env := <-foundCh:
		actualProducer = env.Producer
		foundCorruptIncident = true
	case <-time.After(30 * time.Second):
		foundCorruptIncident = false
	}

	if !foundCorruptIncident {
		t.Fatalf("Watchdog failed to publish corrupt marker event to DLQ in integration test")
	}

	if actualProducer != expectedProducer {
		t.Fatalf("Watchdog sharding failure: expected watchdog from %s to process worker %s, but got processed by %s", expectedProducer, workerID, actualProducer)
	}

	markerKey := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errType)
	getResp, err := eClient.Get(ctx, markerKey)
	if err == nil && len(getResp.Kvs) == 0 {
		err = fmt.Errorf("not found")
	}
	if err != nil {
		t.Fatalf("expected marker in etcd for %s, found none", workerID)
	}

	testutils.LogInfo(t, "✅ Integration test passed: Corrupt FSM & Sharding verified against live etcd & Kafka")
}

func TestIntegration_Incident_KafkaPublishFailureRollback(t *testing.T) {
	testutils.WipeTestState(t, "etcd")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	eClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{testutils.DefaultEtcdUrls},
		DialTimeout: 5 * time.Second,
	})
	defer eClient.Close()

	// 1. Target CP node "cp-b"
	targetCP := "cp-b"
	workerID := testutil.GetWorkerForCP(t, targetCP, []string{"cp-a", "cp-b"})

	// Connect directly to the global test cluster
	var client aegisv1.ControlPlaneTelemetryClient
	for _, c := range globalClients {
		if c != nil {
			client = c // We can use any client as it routes
			break
		}
	}
	if client == nil {
		t.Fatal("no active CP client found")
	}

	// 2. Generate an oversized correlation ID (> 1.5MB) to pass through 10MB gRPC limit
	// but fail Kafka's 1MB default message.max.bytes limit.
	// This dynamically exploits the configured limits without fragility.
	oversizedString := strings.Repeat("A", 1500000) // 1.5 MB

	// Wait for the worker to heartbeat healthily to seed the node ring ownership.
	// Not strictly required for synthetic failures but good for reliability.

	// NOTE: We MUST configure the client to allow sending oversized messages as well!
	// Wait, grpc.NewClient by default has unlimited Send limit?
	// No, default Send limit is math.MaxInt32 (unlimited). Default Recv limit is 4MB.
	stream, err := client.StreamTelemetry(ctx)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	err = stream.Send(&aegisv1.AgentTelemetry{
		WorkerId:         workerID,
		Timestamp:        time.Now().UTC().Format(time.RFC3339Nano),
		SyntheticFailure: "synthetic_test_flag",
		CorrelationId:    oversizedString, // Trigger Kafka rejection
	})
	if err != nil {
		t.Fatalf("failed to send oversized telemetry over gRPC: %v", err)
	}

	_ = stream.CloseSend()

	// We expect to eventually receive an error on the stream due to the Kafka publish failure.
	recvErrChan := make(chan error, 1)
	go func() {
		for {
			_, err := stream.Recv()
			if err != nil {
				recvErrChan <- err
				return
			}
		}
	}()

	select {
	case err := <-recvErrChan:
		testutils.LogInfo(t, "Received expected gRPC error due to Kafka rejection: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for gRPC error from Kafka rejection")
	}

	// 3. Verify etcd FSM Rollback
	// Because the Kafka publish failed, the manager's deferred rollback should have fired
	// and deleted the state (since it was HEALTHY before).
	ws, _ := getWorkerState(ctx, eClient, workerID, string(state.FailureSynthetic))
	if ws != nil && ws.CurrentState != "" {
		t.Fatalf("Expected etcd state to be rolled back (deleted/HEALTHY), but found leaked state: %s", ws.CurrentState)
	}

	testutils.LogInfo(t, "✅ Integration test passed: Kafka publish failure rolled back etcd FSM state properly")
}

func TestIntegration_Incident_CorruptFSM_RejectsValidTransitions(t *testing.T) {
	testutils.WipeTestState(t, "etcd")

	eClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{testutils.DefaultEtcdUrls},
		DialTimeout: 5 * time.Second,
	})
	defer eClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	incidentID := "inc_corrupt_intg_1_" + fmt.Sprint(time.Now().UnixNano())
	errType := string(state.FailureLatencySpike)

	// 1. Manually insert Corrupt DLQ marker and current state in etcd
	markerKey := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errType)
	eClient.Put(ctx, markerKey, string("illegal transition manually injected"))

	ws := membership.WorkerState{
		WorkerID:      workerID,
		ErrorType:     errType,
		IncidentID:    incidentID,
		CurrentState:  string(state.WorkerPostmortemRequested),
		CorrelationID: "corr-" + fmt.Sprint(time.Now().UnixNano()),
	}
	wsData, _ := json.Marshal(ws)
	stateKey := fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errType)
	eClient.Put(ctx, stateKey, string(wsData))

	// 2. Publish a perfectly valid event to advance the FSM (PostmortemRequested -> PostmortemGenerated)
	env := state.EventEnvelope{
		EventType:     incident.TopicPostmortemGenerated,
		IncidentID:    incidentID,
		WorkerID:      workerID,
		CorrelationID: "corr-" + fmt.Sprint(time.Now().UnixNano()),
		Payload: map[string]any{
			"failure_type": errType,
		},
	}
	publishRawMessage(t, incident.TopicPostmortemGenerated, env)

	// 3. Wait a moment for Kafka consumption
	time.Sleep(2 * time.Second)

	// 4. Verify that the FSM was frozen and the transition dropped
	frozenState, err := getWorkerState(ctx, eClient, workerID, errType)
	if err != nil {
		t.Fatalf("failed to read frozen state: %v", err)
	}
	if frozenState.CurrentState != string(state.WorkerPostmortemRequested) {
		t.Fatalf("Expected FSM to be frozen at %s, but got %s", state.WorkerPostmortemRequested, frozenState.CurrentState)
	}

	testutils.LogInfo(t, "✅ Integration test passed: Corrupt FSM successfully rejected valid Kafka transition")
}

func TestIntegration_Incident_FSMOutOfOrderDeferral(t *testing.T) {
	testutils.WipeTestState(t, "etcd")
	eClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{testutils.DefaultEtcdUrls},
		DialTimeout: 5 * time.Second,
	})
	defer eClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	incidentID := "inc_outoforder_" + fmt.Sprint(time.Now().UnixNano())
	errType := string(state.FailureSynthetic)

	// 1. Seed state as POSTMORTEM_REQUESTED
	ws := membership.WorkerState{
		WorkerID:      workerID,
		ErrorType:     errType,
		IncidentID:    incidentID,
		CurrentState:  string(state.WorkerPostmortemRequested),
		CorrelationID: "corr-" + fmt.Sprint(time.Now().UnixNano()),
		UpdatedAt:     time.Now().UTC(),
	}
	wsData, _ := json.Marshal(ws)
	stateKey := fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errType)
	eClient.Put(ctx, stateKey, string(wsData))

	testutils.LogInfo(t, "📖 Publishing early DELIVERED event...")
	// 2. Publish DELIVERED event EARLY
	payload := map[string]interface{}{
		"incident_id":    incidentID,
		"sink":           "file",
		"status":         "delivered",
		"correlation_id": "corr-" + fmt.Sprint(time.Now().UnixNano()),
	}
	publishRawMessage(t, incident.TopicDeliveryStatus, payload)

	// Verify deferred event exists in etcd with retry (asynchronous Kafka processing)
	deferKey := fmt.Sprintf("aegis:defer:%s:%s:%s", workerID, errType, incident.TopicDeliveryStatus)
	deferredFound := false
	var lastErr error
	for i := 0; i < 50; i++ { // wait up to 10s
		getResp, err := eClient.Get(ctx, deferKey)
		lastErr = err
		if err == nil && len(getResp.Kvs) > 0 {
			deferredFound = true
			break
		}


		time.Sleep(200 * time.Millisecond)
	}

	if !deferredFound {
		t.Fatalf("Expected early DELIVERED event to be deferred in store, but got error: %v", lastErr)
	}

	// Verify state is still POSTMORTEM_REQUESTED (event was deferred, not processed or corrupted)
	currState, err := getWorkerState(ctx, eClient, workerID, errType)
	if err != nil || currState.CurrentState != string(state.WorkerPostmortemRequested) {
		t.Fatalf("Expected state to remain POSTMORTEM_REQUESTED after early DELIVERED, got: %s", currState.CurrentState)
	}

	// Verify DLQ marker does not exist
	markerKey := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errType)
	if getResp, _ := eClient.Get(ctx, markerKey); len(getResp.Kvs) > 0 {
		t.Fatalf("Expected no DLQ marker, but FSM was corrupted by early DELIVERED event")
	}

	testutils.LogInfo(t, "📖 Publishing slow POSTMORTEM_GENERATED event...")
	// 3. Publish GENERATED event
	env := state.EventEnvelope{
		EventID:       fmt.Sprintf("evt-%d", time.Now().UnixNano()),
		EventType:     incident.TopicPostmortemGenerated,
		IncidentID:    incidentID,
		WorkerID:      workerID,
		Timestamp:     time.Now().UTC(),
		CorrelationID: "corr-" + fmt.Sprint(time.Now().UnixNano()),
		Payload: map[string]interface{}{
			"failure_type": errType,
		},
	}
	publishRawMessage(t, incident.TopicPostmortemGenerated, env)

	// 4. Wait and verify it fast-forwarded all the way to DELIVERED
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		currState, _ := getWorkerState(ctx, eClient, workerID, errType)
		if currState != nil && currState.CurrentState == string(state.WorkerDelivered) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	currState, _ = getWorkerState(ctx, eClient, workerID, errType)
	if currState == nil || currState.CurrentState != string(state.WorkerDelivered) {
		t.Fatalf("Expected state to fast-forward to DELIVERED using deferred event, got: %v", currState)
	}

	testutils.LogInfo(t, "✅ Integration test passed: FSM correctly deferred out-of-order events without corruption")
}

func TestIntegration_Incident_FSMDeferredEventTimeout(t *testing.T) {
	testutils.WipeTestState(t, "etcd")
	eClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{testutils.DefaultEtcdUrls},
		DialTimeout: 5 * time.Second,
	})
	defer eClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	incidentID := "inc_defer_timeout_" + fmt.Sprint(time.Now().UnixNano())
	errType := string(state.FailureSynthetic)

	testutils.LogInfo(t, "📖 Starting Kafka readers for DLQ topic...")
	foundCh := waitForDLQEvent(incident.TopicCorruptFSMDLQ, func(env state.EventEnvelope) bool {
		return env.WorkerID == workerID && env.EventType == "aegis.cp.fsm_illegal_transition"
	})

	time.Sleep(1 * time.Second)

	// 1. Seed state as POSTMORTEM_REQUESTED
	ws := membership.WorkerState{
		WorkerID:      workerID,
		ErrorType:     errType,
		IncidentID:    incidentID,
		CurrentState:  string(state.WorkerPostmortemRequested),
		CorrelationID: "corr-" + fmt.Sprint(time.Now().UnixNano()),
		UpdatedAt:     time.Now().UTC(),
	}
	wsData, _ := json.Marshal(ws)
	stateKey := fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errType)
	eClient.Put(ctx, stateKey, string(wsData))

	// Give the watchdog time to sync
	time.Sleep(2 * time.Second)

	// 2. Insert a deferred event that is expiring
	// The watchdog scans for TTL < 2 minutes (120s)
	simulatedTTL := 2 * time.Second
	deferKey := fmt.Sprintf("aegis:defer:%s:%s:%s", workerID, errType, incident.TopicDeliveryStatus)
	func() {
		leaseResp, _ := eClient.Grant(ctx, int64(simulatedTTL.Seconds()))
		eClient.Put(ctx, deferKey, string("{}"), clientv3.WithLease(leaseResp.ID))
	}()

	testutils.LogInfo(t, "📖 Verifying Watchdog published corrupt FSM event due to deferral timeout...")

	foundTimeoutIncident := false
	select {
	case <-foundCh:
		foundTimeoutIncident = true
	case <-time.After(simulatedTTL + (5 * time.Second)):
		foundTimeoutIncident = false
	}

	if !foundTimeoutIncident {
		t.Fatalf("Watchdog failed to publish corrupt FSM event for expired deferral")
	}

	// 3. Verify DLQ marker exists
	markerKey := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errType)
	getResp, err := eClient.Get(ctx, markerKey)
	if err == nil && len(getResp.Kvs) == 0 {
		err = fmt.Errorf("not found")
	}
	if err != nil {
		t.Fatalf("Expected DLQ marker in etcd due to deferral timeout, but got error: %v", err)
	}

	testutils.LogInfo(t, "✅ Integration test passed: Watchdog correctly marked expired deferral as corrupt FSM")
}

func TestIntegration_Incident_FSMConsumerChecksStaleEvents_And_SkipsIfAlreadyHealthyOrResolved(t *testing.T) {
	testutils.WipeTestState(t, "etcd")
	eClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{testutils.DefaultEtcdUrls},
		DialTimeout: 5 * time.Second,
	})
	defer eClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	newIncidentID := "inc_new_123"
	oldIncidentID := "inc_old_999"
	errType := string(state.FailureSynthetic)

	// 1. Seed state as SUSPECTED with new incident ID
	ws := membership.WorkerState{
		WorkerID:      workerID,
		ErrorType:     errType,
		IncidentID:    newIncidentID,
		CurrentState:  string(state.WorkerPostmortemRequested),
		CorrelationID: "corr-" + fmt.Sprint(time.Now().UnixNano()),
		UpdatedAt:     time.Now().UTC(),
	}
	wsData, _ := json.Marshal(ws)
	stateKey := fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errType)
	eClient.Put(ctx, stateKey, string(wsData))
	time.Sleep(2 * time.Second)

	// 2. Publish a stale event (PostmortemGenerated) for OLD incident
	pClient := &segmentiokafka.Writer{
		Addr:     segmentiokafka.TCP(testutils.DefaultKafkaBrokers...),
		Topic:    incident.TopicPostmortemGenerated,
		Balancer: &segmentiokafka.LeastBytes{},
	}
	defer pClient.Close()

	env := state.EventEnvelope{
		EventType:     incident.TopicPostmortemGenerated,
		IncidentID:    oldIncidentID,
		WorkerID:      workerID,
		CorrelationID: "corr-" + fmt.Sprint(time.Now().UnixNano()),
		Payload: map[string]any{
			"failure_type": errType,
		},
	}
	envData, _ := json.Marshal(env)

	err := pClient.WriteMessages(ctx, segmentiokafka.Message{
		Key:   []byte(fmt.Sprintf("%s:%s", workerID, errType)),
		Value: envData,
	})
	if err != nil {
		t.Fatalf("failed to publish stale event: %v", err)
	}

	// Wait to see if it processes
	time.Sleep(3 * time.Second)

	// 3. Verify state did NOT change
	currState, err := getWorkerState(ctx, eClient, workerID, errType)
	if err != nil {
		t.Fatalf("Failed to get state: %v", err)
	}
	if currState.CurrentState != string(state.WorkerPostmortemRequested) {
		t.Fatalf("Expected state to remain POSTMORTEM_REQUESTED, but got: %s", currState.CurrentState)
	}
	if currState.IncidentID != newIncidentID {
		t.Fatalf("Expected incident ID to remain %s, but got: %s", newIncidentID, currState.IncidentID)
	}

	// 4. Publish the EXACT SAME event, but with the CURRENT incident ID (Valid)
	env.IncidentID = newIncidentID
	envDataValid, _ := json.Marshal(env)
	err = pClient.WriteMessages(ctx, segmentiokafka.Message{
		Key:   []byte(fmt.Sprintf("%s:%s", workerID, errType)),
		Value: envDataValid,
	})
	if err != nil {
		t.Fatalf("failed to publish valid event: %v", err)
	}

	// Wait for processing with polling (up to 15s)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		currState, err = getWorkerState(ctx, eClient, workerID, errType)
		if err == nil && currState.CurrentState != string(state.WorkerPostmortemRequested) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 5. Verify state DID change, proving the event payload was valid and only rejected earlier due to IsStale
	currState, err = getWorkerState(ctx, eClient, workerID, errType)
	if err != nil {
		t.Fatalf("Failed to get state: %v", err)
	}
	if currState.CurrentState == string(state.WorkerPostmortemRequested) {
		t.Fatalf("Expected state to transition out of POSTMORTEM_REQUESTED upon receiving a valid event, but it did not. This means the event processing logic is broken, and the previous stale check success was a false positive.")
	}

	// 6. Forcefully set the state to RESOLVED (simulating aegis-cli --force)
	currState.CurrentState = string(state.WorkerResolved)
	wsDataResolved, _ := json.Marshal(currState)
	eClient.Put(ctx, stateKey, string(wsDataResolved))
	time.Sleep(1 * time.Second)

	// 7. Publish a late PostmortemGenerated event with the VALID current incident ID
	env.CorrelationID = "corr-" + fmt.Sprint(time.Now().UnixNano())
	envDataLate, _ := json.Marshal(env)
	err = pClient.WriteMessages(ctx, segmentiokafka.Message{
		Key:   []byte(fmt.Sprintf("%s:%s", workerID, errType)),
		Value: envDataLate,
	})
	if err != nil {
		t.Fatalf("failed to publish late event: %v", err)
	}

	// 8. Wait for processing
	time.Sleep(3 * time.Second)

	// 9. Verify state remains RESOLVED and NO corruption (DLQ marker) occurred
	currStateAfter, err := getWorkerState(ctx, eClient, workerID, errType)
	if err != nil {
		t.Fatalf("Failed to get state after late event: %v", err)
	}
	if currStateAfter.CurrentState != string(state.WorkerResolved) {
		t.Fatalf("Expected state to remain RESOLVED, but got: %s", currStateAfter.CurrentState)
	}

	// Check if a DLQ marker was created (corruption)
	dlqResp, err := eClient.Get(ctx, fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errType))
	dlqMarker := ""
	if err == nil && len(dlqResp.Kvs) > 0 {
		dlqMarker = string(dlqResp.Kvs[0].Value)
	}
	if err == nil && dlqMarker != "" {
		t.Fatalf("Expected NO DLQ marker, but found corruption marker: %s", dlqMarker)
	}

	testutils.LogInfo(t, "✅ Integration test passed: Consumer correctly ignored stale Kafka event, accepted valid one, and gracefully skipped late event after manual resolution")
}

func TestIntegration_Incident_WatchdogChecksStaleDeferral(t *testing.T) {
	testutils.WipeTestState(t, "etcd")
	eClient, _ := clientv3.New(clientv3.Config{
		Endpoints:   []string{testutils.DefaultEtcdUrls},
		DialTimeout: 5 * time.Second,
	})
	defer eClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	newIncidentID := "inc_new_123"
	oldIncidentID := "inc_old_999"
	errType := string(state.FailureSynthetic)

	// 1. Seed state as SUSPECTED with NEW incident ID
	ws := membership.WorkerState{
		WorkerID:      workerID,
		ErrorType:     errType,
		IncidentID:    newIncidentID,
		CurrentState:  string(state.WorkerSuspected),
		CorrelationID: "corr-" + fmt.Sprint(time.Now().UnixNano()),
		UpdatedAt:     time.Now().UTC(),
	}
	wsData, _ := json.Marshal(ws)
	stateKey := fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errType)
	eClient.Put(ctx, stateKey, string(wsData))
	time.Sleep(2 * time.Second)

	// 2. Insert a deferred event that is expiring, but for the OLD incident ID
	simulatedTTL := 2 * time.Second
	deferKey := fmt.Sprintf("aegis:defer:%s:%s:%s", workerID, errType, incident.TopicDeliveryStatus)

	// Payload for old incident
	defPayload := map[string]any{
		"incident_id": oldIncidentID,
		"status":      "delivered",
	}
	defData, _ := json.Marshal(defPayload)
	func() {
		leaseResp, _ := eClient.Grant(ctx, int64(simulatedTTL.Seconds()))
		eClient.Put(ctx, deferKey, string(defData), clientv3.WithLease(leaseResp.ID))
	}()

	// Wait for watchdog to process the expiring deferral
	time.Sleep(simulatedTTL + (3 * time.Second))

	// 3. Verify deferral was DELETED, but FSM state was NOT marked as corrupt
	// Check deferral is gone
	getResp, err := eClient.Get(ctx, deferKey)
	if err == nil && len(getResp.Kvs) == 0 {
		err = fmt.Errorf("not found")
	}
	if err == nil {
		t.Fatalf("Expected stale deferral to be deleted, but it still exists")
	}

	// Check state is still SUSPECTED
	currState, err := getWorkerState(ctx, eClient, workerID, errType)
	if err != nil {
		t.Fatalf("Failed to get state: %v", err)
	}
	if currState.CurrentState != string(state.WorkerSuspected) {
		t.Fatalf("Expected state to remain SUSPECTED, but got: %s (Watchdog mistakenly corrupted it?)", currState.CurrentState)
	}

	// 4. Insert the EXACT SAME deferred event, but with the CURRENT incident ID (Valid)
	defPayloadValid := map[string]any{
		"incident_id": newIncidentID,
		"status":      "delivered",
	}
	defDataValid, _ := json.Marshal(defPayloadValid)
	func() {
		leaseResp, _ := eClient.Grant(ctx, int64(simulatedTTL.Seconds()))
		eClient.Put(ctx, deferKey, string(defDataValid), clientv3.WithLease(leaseResp.ID))
	}()

	// Wait for watchdog to process the valid expiring deferral
	time.Sleep(simulatedTTL + (5 * time.Second))

	// 5. Verify state was marked as corrupt (DLQ marker created), proving the watchdog logic works and only rejected earlier due to IsStale
	markerKey := fmt.Sprintf("aegis:cp:dlq:corrupt:%s:%s", workerID, errType)
	getResp, err = eClient.Get(ctx, markerKey)
	if err == nil && len(getResp.Kvs) == 0 {
		err = fmt.Errorf("not found")
	}
	if err != nil {
		t.Fatalf("Expected DLQ marker to be created upon expiring a valid deferral, but it was not (err: %v). This means watchdog processing is broken, and the previous stale check success was a false positive.", err)
	}

	testutils.LogInfo(t, "✅ Integration test passed: Watchdog correctly ignored stale expiring deferral but reacted to valid one")
}
