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
	"github.com/redis/go-redis/v9"
	segmentiokafka "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	incidentFactory *testutil.CPTestClusterFactory
	globalClients   []aegisv1.ControlPlaneTelemetryClient
)

func TestMain(m *testing.M) {
	incidentFactory = testutil.NewCPTestClusterFactory(
		[]string{"cp-a", "cp-b"},
		testutils.DefaultRedisAddr,
		testutils.DefaultKafkaBrokers,
		map[string]string{
			"AEGIS_FSM_WATCHDOG_INTERVAL_SECONDS": "1",
		},
	)

	// Start the cluster once.
	globalClients = incidentFactory.StartCluster(nil)

	code := m.Run()

	incidentFactory.Teardown()
	os.Exit(code)
}

func getWorkerState(ctx context.Context, rClient *redis.Client, workerID, errorType string) (*membership.WorkerState, error) {
	key := fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errorType)
	data, err := rClient.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	var ws membership.WorkerState
	if err := json.Unmarshal([]byte(data), &ws); err != nil {
		return nil, err
	}
	return &ws, nil
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
	testutils.WipeTestState(t, "redis kafka:aegis.incident.detected,aegis.dlq,aegis.diagnostics.requested,aegis.diagnostics.collected,aegis.postmortem.requested")
	client := globalClients[0]

	rClient := redis.NewClient(&redis.Options{Addr: testutils.DefaultRedisAddr})
	defer rClient.Close()

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
		CorrelationId:    "corr-fsm-1",
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

	// 2. Wait for POSTMORTEM_REQUESTED state in Redis
	var workerState *membership.WorkerState
	found := false
	for i := 0; i < 50; i++ {
		workerState, _ = getWorkerState(ctx, rClient, workerID, string(state.FailureSynthetic))
		if workerState != nil && workerState.CurrentState == string(state.WorkerPostmortemRequested) {
			found = true
			incidentID = workerState.IncidentID // Capture deterministic ID
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !found {
		t.Fatalf("expected state POSTMORTEM_REQUESTED in Redis, got %v", workerState)
	}

	// Helper to push mock event and wait for redis transition
	assertTransition := func(topic string, eventType string, newState state.WorkerHealthState) {
		if topic == incident.TopicDeliveryStatus {
			payload := map[string]interface{}{
				"incident_id":    incidentID,
				"sink":           "file",
				"status":         "delivered",
				"correlation_id": "corr-fsm-1",
			}
			publishRawMessage(t, topic, payload)
		} else {
			env := state.EventEnvelope{
				EventID:       fmt.Sprintf("evt-%d", time.Now().UnixNano()),
				EventType:     eventType,
				IncidentID:    incidentID,
				WorkerID:      workerID,
				Timestamp:     time.Now().UTC(),
				CorrelationID: "corr-fsm-1",
				Payload: map[string]interface{}{
					"state":  string(newState),
					"status": "delivered",
				},
			}
			publishRawMessage(t, topic, env)
		}

		found = false
		for i := 0; i < 150; i++ {
			workerState, _ = getWorkerState(ctx, rClient, workerID, string(state.FailureSynthetic))
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

	assertTransition(incident.TopicDeliveryStatus, "aegis.sink.delivered", state.WorkerResolved)

	testutils.LogInfo(t, "✅ Integration test passed: FSM transitions verified against live Redis & Kafka")
}

func TestIntegration_Incident_StuckFSM_WatchdogAlert(t *testing.T) {
	testutils.WipeTestState(t, "redis kafka:aegis.incident.detected,aegis.dlq,aegis.diagnostics.requested,aegis.diagnostics.collected,aegis.postmortem.requested")
	// client not directly used, but server is running

	rClient := redis.NewClient(&redis.Options{Addr: testutils.DefaultRedisAddr})
	defer rClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	incidentID := fmt.Sprintf("inc-%d", time.Now().UnixNano())
	errType := state.FailureSynthetic

	testutils.LogInfo(t, "📖 Starting Kafka readers for DLQ topic...")
	topic := incident.TopicCorruptFSMDLQ
	numPartitions := testutils.GetTopicPartitionCount(topic)

	foundCh := make(chan bool, 1)
	for p := 0; p < numPartitions; p++ {
		go func(partition int) {
			reader := segmentiokafka.NewReader(segmentiokafka.ReaderConfig{
				Brokers:   testutils.DefaultKafkaBrokers,
				Topic:     topic,
				Partition: partition,
			})
			_ = reader.SetOffset(segmentiokafka.LastOffset)

			rCtx, rCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer rCancel()
			defer reader.Close()

			for {
				m, err := reader.ReadMessage(rCtx)
				if err != nil {
					break
				}
				var env state.EventEnvelope
				if err := json.Unmarshal(m.Value, &env); err == nil && env.WorkerID == workerID {
					if env.EventType == "aegis.cp.fsm_stuck_incident" {
						select {
						case foundCh <- true:
						default:
						}
						break
					}
				}
			}
		}(p)
	}

	// Give the readers a moment to connect and set offset
	time.Sleep(1 * time.Second)

	// Fake an old state in Redis to trigger stuck metric
	oldTime := time.Now().UTC().Add(-20 * time.Minute)
	err := rClient.Set(ctx, fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errType),
		fmt.Sprintf(`{"worker_id":"%s","error_type":"%s","current_state":"%s","incident_id":"%s","correlation_id":"corr-wd-1","updated_at":"%s"}`,
			workerID, errType, state.WorkerDiagnosticsTriggered, incidentID, oldTime.Format(time.RFC3339Nano)),
		0).Err()
	if err != nil {
		t.Fatalf("Failed to manipulate Redis state for watchdog: %v", err)
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
	testutils.LogInfo(t, "✅ Integration test passed: Watchdog behavior verified against live Redis & Kafka")
}

func TestIntegration_Incident_CorruptFSM_WatchdogAlert_And_Sharding(t *testing.T) {
	testutils.WipeTestState(t, "redis kafka:aegis.incident.detected,aegis.dlq,aegis.diagnostics.requested,aegis.diagnostics.collected,aegis.postmortem.requested,aegis.postmortem.generated")

	rClient := redis.NewClient(&redis.Options{Addr: testutils.DefaultRedisAddr})
	defer rClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	expectedProducer := state.CreateProducerRef("cp-a")
	errType := string(state.FailureSynthetic)

	testutils.LogInfo(t, "📖 Starting Kafka readers for DLQ topic...")
	topic := incident.TopicCorruptFSMDLQ
	numPartitions := testutils.GetTopicPartitionCount(topic)

	foundCh := make(chan string, 1)
	for p := 0; p < numPartitions; p++ {
		go func(partition int) {
			reader := segmentiokafka.NewReader(segmentiokafka.ReaderConfig{
				Brokers:   testutils.DefaultKafkaBrokers,
				Topic:     topic,
				Partition: partition,
			})
			_ = reader.SetOffset(segmentiokafka.LastOffset)

			rCtx, rCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer rCancel()
			defer reader.Close()

			for {
				m, err := reader.ReadMessage(rCtx)
				if err != nil {
					break
				}
				var env state.EventEnvelope
				if err := json.Unmarshal(m.Value, &env); err == nil && (env.WorkerID == workerID || strings.Contains(env.CausationID, workerID)) {
					if env.EventType == "aegis.cp.fsm_corrupt_marker" {
						select {
						case foundCh <- env.Producer:
						default:
						}
						break
					}
				}
			}
		}(p)
	}

	time.Sleep(2 * time.Second)

	incidentID := "inc_poison_pill"
	err := rClient.Set(ctx, fmt.Sprintf("aegis:cp:worker:state:%s:%s", workerID, errType),
		fmt.Sprintf(`{"worker_id":"%s","error_type":"%s","current_state":"%s","incident_id":"%s","correlation_id":"corr-wd-1","updated_at":"%s"}`,
			workerID, errType, state.WorkerSuspected, incidentID, time.Now().UTC().Format(time.RFC3339Nano)),
		0).Err()
	if err != nil {
		t.Fatalf("Failed to seed Redis state: %v", err)
	}

	// Give the FSMConsumer in the CP processes enough time to join the Kafka consumer group and subscribe.
	time.Sleep(10 * time.Second)

	testutils.LogInfo(t, "📖 Publishing poison pill to trigger FSM corruption...")
	
	poisonPill := state.EventEnvelope{
		EventType: incident.TopicPostmortemGenerated,
		WorkerID:  workerID,
		Payload: map[string]any{
			"worker_id":    workerID,
			"failure_type": errType,
		},
	}
	publishRawMessage(t, incident.TopicPostmortemGenerated, poisonPill)

	testutils.LogInfo(t, "📖 Verifying DLQ marker was created in Redis and DLQ event was published...")

	foundCorruptIncident := false
	var actualProducer string
	select {
	case actualProducer = <-foundCh:
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
	_, err = rClient.Get(ctx, markerKey).Result()
	if err != nil {
		t.Fatalf("expected marker in Redis for %s, found none", workerID)
	}

	testutils.LogInfo(t, "✅ Integration test passed: Corrupt FSM & Sharding verified against live Redis & Kafka")
}
