//go:build integration

package server_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	aegisv1 "github.com/aegis/aegis/gen/go/aegis/v1"
	"github.com/aegis/aegis/services/control-plane/internal/state"
	"github.com/aegis/aegis/services/control-plane/internal/testutil"
	"github.com/aegis/aegis/tests/integration/testutils"
	segmentiokafka "github.com/segmentio/kafka-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	serverFactory *testutil.CPTestClusterFactory
	globalClients []aegisv1.ControlPlaneTelemetryClient
)

func TestMain(m *testing.M) {
	serverFactory = testutil.NewCPTestClusterFactory(
		[]string{"cp-a", "cp-b"},
		testutils.DefaultRedisAddr,
		testutils.DefaultKafkaBrokers,
		nil,
	)

	// Start the cluster once.
	// The factory internally uses sync.Once. We pass nil for t since it's TestMain.
	globalClients = serverFactory.StartCluster(nil)

	code := m.Run()

	serverFactory.Teardown()
	os.Exit(code)
}

func readExpectedEvents(t *testing.T, brokers []string, workerID string, expectedTopics []string) []state.EventEnvelope {
	t.Helper()
	testutils.LogInfo(t, "📖 Waiting for %d expected events on Kafka for worker %s...", len(expectedTopics), workerID)
	var events []state.EventEnvelope
	for _, topic := range expectedTopics {
		found := make(chan state.EventEnvelope, 1)

		numPartitions := testutils.GetTopicPartitionCount(topic)
		for p := 0; p < numPartitions; p++ {
			go func(partition int) {
				reader := segmentiokafka.NewReader(segmentiokafka.ReaderConfig{
					Brokers:   brokers,
					Topic:     topic,
					Partition: partition,
				})
				_ = reader.SetOffset(segmentiokafka.FirstOffset)
				defer reader.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				for {
					m, err := reader.ReadMessage(ctx)
					if err != nil {
						if ctx.Err() != nil {
							return
						}
						time.Sleep(100 * time.Millisecond)
						continue
					}
					var env state.EventEnvelope
					if err := json.Unmarshal(m.Value, &env); err == nil && env.WorkerID == workerID {
						select {
						case found <- env:
						default:
						}
						return
					}
				}
			}(p)
		}

		select {
		case env := <-found:
			events = append(events, env)
		case <-time.After(10 * time.Second):
			t.Fatalf("failed to read expected event from %s: timeout (worker %s)", topic, workerID)
		}
	}
	return events
}



func assertNoEvents(t *testing.T, brokers []string, workerID string, topic string) {
	t.Helper()
	testutils.LogInfo(t, "🛡️ Verifying NO events are published for worker %s on %s...", workerID, topic)
	found := make(chan struct{}, 1)

	numPartitions := testutils.GetTopicPartitionCount(topic)
	for p := 0; p < numPartitions; p++ {
		go func(partition int) {
			reader := segmentiokafka.NewReader(segmentiokafka.ReaderConfig{
				Brokers:   brokers,
				Topic:     topic,
				Partition: partition,
			})
			_ = reader.SetOffset(segmentiokafka.FirstOffset)
			defer reader.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			for {
				m, err := reader.ReadMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					time.Sleep(100 * time.Millisecond)
					continue
				}
				var env state.EventEnvelope
				if err := json.Unmarshal(m.Value, &env); err == nil && env.WorkerID == workerID {
					select {
					case found <- struct{}{}:
					default:
					}
					return
				}
			}
		}(p)
	}

	select {
	case <-found:
		t.Fatalf("expected no events for worker %s on %s, but found one", workerID, topic)
	case <-time.After(10 * time.Second):
		// Success! No events found within 10 seconds.
	}
}

func TestIntegration_StreamTelemetry_Accepted(t *testing.T) {
	testutils.WipeTestState(t, "redis")
	client := globalClients[0]

	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})

	if err := stream.Send(&aegisv1.AgentTelemetry{
		WorkerId:           workerID,
		Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
		GpuUtilization:     0.5,
		TemperatureCelsius: 60,
		ModelServerHealthy: true,
		CorrelationId:      "corr-1",
	}); err != nil {
		t.Fatal(err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if resp.DirectiveType != "accepted" {
		t.Fatalf("expected accepted, got %s", resp.DirectiveType)
	}
	if resp.CorrelationId != "corr-1" {
		t.Fatalf("correlation_id not preserved: got %s", resp.CorrelationId)
	}
}

func TestIntegration_StreamTelemetry_Redirect(t *testing.T) {
	testutils.WipeTestState(t, "redis")
	// Two CP members; worker hashing will route to one of them.
	client := globalClients[0]

	// Try enough workers to find one that hashes to cp-b (a redirect).
	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var gotRedirect bool
	for i := 0; i < 100; i++ {
		workerID := "redirect-probe-" + time.Now().Format("150405.000000000") + "-" + string(rune('A'+i%26))
		if err := stream.Send(&aegisv1.AgentTelemetry{
			WorkerId:           workerID,
			Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
			ModelServerHealthy: true,
		}); err != nil {
			t.Fatal(err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if resp.DirectiveType == "redirect" {
			gotRedirect = true
			if resp.OwnerHint != "cp-b:50051" {
				t.Fatalf("expected owner hint cp-b:50051, got %s", resp.OwnerHint)
			}
			break
		}
	}
	if !gotRedirect {
		t.Fatal("expected at least one worker to redirect to cp-b")
	}
}

func TestIntegration_StreamTelemetry_ResourceExhausted(t *testing.T) {
	testutils.WipeTestState(t, "redis kafka:aegis.incident.detected,aegis.diagnostics.requested")
	client := globalClients[0]

	// Since queue size is 256 globally, we need to flood the queue to exhaust it.
	// We'll open a stream and send >256 messages rapidly without waiting for Recv.
	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})
	var gotExhausted bool
	for i := 0; i < 300; i++ {
		if err := stream.Send(&aegisv1.AgentTelemetry{
			WorkerId:           workerID,
			Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
			ModelServerHealthy: true,
		}); err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.ResourceExhausted {
				gotExhausted = true
				break
			}
			break
		}
	}

	// Also check Recv for the async error.
	if !gotExhausted {
		for i := 0; i < 300; i++ {
			_, err := stream.Recv()
			if err != nil {
				st, ok := status.FromError(err)
				if ok && st.Code() == codes.ResourceExhausted {
					gotExhausted = true
				}
				break
			}
		}
	}

	if !gotExhausted {
		t.Fatal("expected ResourceExhausted error by flooding queue")
	}

	// Drain the queue before finishing the test so we don't break subsequent tests.
	// Since we flooded the queue, the gRPC server is still processing them in the background.
	// We will probe it until it accepts a new message.
	for i := 0; i < 50; i++ {
		drainStream, err := client.StreamTelemetry(context.Background())
		if err == nil {
			err = drainStream.Send(&aegisv1.AgentTelemetry{
				WorkerId:           workerID,
				Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
				ModelServerHealthy: true,
			})
			if err == nil {
				resp, rErr := drainStream.Recv()
				if rErr == nil && resp.DirectiveType == "accepted" {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Give the background workers an extra moment to fully drain the remaining items in the queue
	time.Sleep(500 * time.Millisecond)
}

func TestIntegration_TelemetryProcessing_IncidentDetected(t *testing.T) {
	testutils.WipeTestState(t, "redis kafka:aegis.incident.detected")
	client := globalClients[0]

	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})

	// Send 3 samples with sustained high temperature (>=85C) to trigger detection.
	for i := 0; i < 3; i++ {
		if err := stream.Send(&aegisv1.AgentTelemetry{
			WorkerId:           workerID,
			Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
			TemperatureCelsius: 90,
			ModelServerHealthy: true,
			CorrelationId:      "corr-overheat",
		}); err != nil {
			t.Fatal(err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if resp.DirectiveType != "accepted" {
			t.Fatalf("sample %d: expected accepted, got %s", i, resp.DirectiveType)
		}
	}

	// Allow background processing to complete.
	time.Sleep(200 * time.Millisecond)

	expectedTopics := []string{
		"aegis.incident.detected",
		"aegis.diagnostics.requested",
		"aegis.diagnostics.collected",
		"aegis.postmortem.requested",
	}

	events := readExpectedEvents(t, testutils.DefaultKafkaBrokers, workerID, expectedTopics)

	// Verify envelope fields are populated.
	env := events[0]
	if env.WorkerID != workerID {
		t.Errorf("expected worker_id=%s, got %s", workerID, env.WorkerID)
	}
	if env.CorrelationID != "corr-overheat" {
		t.Errorf("expected correlation_id=corr-overheat, got %s", env.CorrelationID)
	}
	if env.IncidentID == "" {
		t.Error("incident_id should not be empty")
	}
	if env.SchemaVersion != "aegis.events.v1" {
		t.Errorf("expected schema_version=aegis.events.v1, got %s", env.SchemaVersion)
	}
}

func TestIntegration_TelemetryProcessing_HealthyNoIncident(t *testing.T) {
	testutils.WipeTestState(t, "redis kafka:aegis.incident.detected,aegis.diagnostics.requested")
	client := globalClients[0]

	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})

	// Send healthy telemetry — no incident should be created.
	for i := 0; i < 5; i++ {
		if err := stream.Send(&aegisv1.AgentTelemetry{
			WorkerId:           workerID,
			Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
			GpuUtilization:     0.4,
			TemperatureCelsius: 55,
			ModelServerHealthy: true,
			VramUsedBytes:      20,
			VramTotalBytes:     100,
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(200 * time.Millisecond)
	assertNoEvents(t, testutils.DefaultKafkaBrokers, workerID, "aegis.incident.detected")
}

func TestIntegration_TelemetryProcessing_ModelUnhealthyImmediate(t *testing.T) {
	testutils.WipeTestState(t, "redis kafka:aegis.incident.detected")
	client := globalClients[0]

	stream, err := client.StreamTelemetry(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	workerID := testutil.GetWorkerForCP(t, "cp-a", []string{"cp-a", "cp-b"})

	// Model server unhealthy triggers immediately (no sustained count needed).
	if err := stream.Send(&aegisv1.AgentTelemetry{
		WorkerId:           workerID,
		Timestamp:          time.Now().UTC().Format(time.RFC3339Nano),
		ModelServerHealthy: false,
		CorrelationId:      "corr-crash",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(200 * time.Millisecond)

	events := readExpectedEvents(t, testutils.DefaultKafkaBrokers, workerID, []string{"aegis.incident.detected"})
	payload := events[0].Payload
	if ft, ok := payload["failure_type"]; ok {
		if ft != "model_unhealthy" {
			t.Errorf("expected failure_type=model_unhealthy, got %v", ft)
		}
	}
}

func TestIntegration_SubmitDiagnostics_Accepted(t *testing.T) {
	testutils.WipeTestState(t, "redis")
	client := globalClients[0]

	ack, err := client.SubmitDiagnostics(context.Background(), &aegisv1.DiagnosticBundle{
		WorkerId:         "worker-1",
		IncidentId:       "inc_test123",
		DiagnosticStatus: "complete",
		JsonPayload:      `{"ok":true}`,
		CorrelationId:    "corr-diag",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ack.Accepted {
		t.Fatalf("expected accepted=true, got %v", ack.Accepted)
	}
}

func TestIntegration_SubmitDiagnostics_InvalidArgument(t *testing.T) {
	testutils.WipeTestState(t, "")
	client := globalClients[0]

	// Missing required fields.
	_, err := client.SubmitDiagnostics(context.Background(), &aegisv1.DiagnosticBundle{})
	if err == nil {
		t.Fatal("expected error for empty worker_id/incident_id")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}
