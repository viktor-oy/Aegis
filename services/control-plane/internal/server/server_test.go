package server

import (
	"context"
	"github.com/aegis/aegis/services/control-plane/internal/testutil"
	"testing"
	"time"

	aegisv1 "github.com/aegis/aegis/gen/go/aegis/v1"
	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type staticRing struct {
	member hashring.Member
}

func (r staticRing) Owner(string) (hashring.Member, bool) { return r.member, true }

type okDiagnostics struct{}

func (okDiagnostics) TriggerDiagnostics(_ context.Context, req state.DiagnosticRequest) (state.DiagnosticBundle, error) {
	return state.DiagnosticBundle{
		WorkerID:         req.WorkerID,
		IncidentID:       req.IncidentID,
		CollectedAt:      req.RequestedAt,
		DiagnosticStatus: "complete",
		Payload:          map[string]any{"ok": true},
		CorrelationID:    req.CorrelationID,
	}, nil
}

func TestOwnerRedirect(t *testing.T) {
	cp := New("cp-a", "a:50051", staticRing{member: hashring.Member{ID: "cp-b", Address: "b:50051"}}, nil, 1, time.Second)
	directive, err := cp.Ingest(context.Background(), state.TelemetrySample{WorkerID: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	if directive.Type != "redirect" || directive.OwnerHint != "b:50051" {
		t.Fatalf("unexpected directive %#v", directive)
	}
}

func TestResourceExhausted(t *testing.T) {
	store := testutil.NewMockStore()
	manager := incident.NewManager(store, &testutil.MockPublisher{}, okDiagnostics{}, "cp-a", time.Minute)
	cp := New("cp-a", "a:50051", staticRing{member: hashring.Member{ID: "cp-a", Address: "a:50051"}}, manager, 1, time.Second)
	sample := state.TelemetrySample{WorkerID: "worker-a", Timestamp: time.Now(), ReceivedAt: time.Now(), ModelServerHealthy: true}
	if _, err := cp.Ingest(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.Ingest(context.Background(), sample); err != ErrResourceExhausted {
		t.Fatalf("expected resource exhausted, got %v", err)
	}
}

func TestAcceptedDirective(t *testing.T) {
	store := testutil.NewMockStore()
	manager := incident.NewManager(store, &testutil.MockPublisher{}, okDiagnostics{}, "cp-a", time.Minute)
	cp := New("cp-a", "a:50051", staticRing{member: hashring.Member{ID: "cp-a", Address: "a:50051"}}, manager, 64, time.Second)
	sample := state.TelemetrySample{
		WorkerID:           "worker-a",
		Timestamp:          time.Now(),
		ModelServerHealthy: true,
		CorrelationID:      "corr-test",
	}
	directive, err := cp.Ingest(context.Background(), sample)
	if err != nil {
		t.Fatal(err)
	}
	if directive.Type != "accepted" {
		t.Fatalf("expected accepted, got %s", directive.Type)
	}
	if directive.CorrelationID != "corr-test" {
		t.Fatalf("expected correlation_id=corr-test, got %s", directive.CorrelationID)
	}
}

func TestQueueDepth(t *testing.T) {
	store := testutil.NewMockStore()
	manager := incident.NewManager(store, &testutil.MockPublisher{}, okDiagnostics{}, "cp-a", time.Minute)
	cp := New("cp-a", "a:50051", staticRing{member: hashring.Member{ID: "cp-a", Address: "a:50051"}}, manager, 64, time.Second)

	if cp.QueueDepth() != 0 {
		t.Fatalf("expected queue depth 0, got %d", cp.QueueDepth())
	}

	sample := state.TelemetrySample{WorkerID: "worker-a", Timestamp: time.Now(), ReceivedAt: time.Now(), ModelServerHealthy: true}
	cp.Ingest(context.Background(), sample)

	if cp.QueueDepth() != 1 {
		t.Fatalf("expected queue depth 1, got %d", cp.QueueDepth())
	}
}

func TestProcessOneDrainsQueue(t *testing.T) {
	store := testutil.NewMockStore()
	manager := incident.NewManager(store, &testutil.MockPublisher{}, okDiagnostics{}, "cp-a", time.Minute)
	cp := New("cp-a", "a:50051", staticRing{member: hashring.Member{ID: "cp-a", Address: "a:50051"}}, manager, 64, time.Second)

	sample := state.TelemetrySample{WorkerID: "worker-a", Timestamp: time.Now(), ReceivedAt: time.Now(), ModelServerHealthy: true}
	cp.Ingest(context.Background(), sample)

	processed, err := cp.ProcessOne(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !processed {
		t.Fatal("expected to process one item")
	}
	if cp.QueueDepth() != 0 {
		t.Fatalf("expected queue depth 0 after processing, got %d", cp.QueueDepth())
	}
}

func TestProcessOneEmptyQueue(t *testing.T) {
	store := testutil.NewMockStore()
	manager := incident.NewManager(store, &testutil.MockPublisher{}, okDiagnostics{}, "cp-a", time.Minute)
	cp := New("cp-a", "a:50051", staticRing{member: hashring.Member{ID: "cp-a", Address: "a:50051"}}, manager, 64, time.Second)

	processed, err := cp.ProcessOne(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if processed {
		t.Fatal("expected nothing to process from empty queue")
	}
}

// Unit tests for gRPC type conversion functions.

func TestTelemetryFromProto(t *testing.T) {
	ts := time.Date(2026, 3, 10, 14, 0, 0, 0, time.UTC)
	msg := &aegisv1.AgentTelemetry{
		WorkerId:           "worker-1",
		Timestamp:          ts.Format(time.RFC3339Nano),
		GpuUtilization:     0.75,
		VramUsedBytes:      8000000000,
		VramTotalBytes:     16000000000,
		TemperatureCelsius: 72.5,
		PowerWatts:         250.0,
		EccErrorCount:      3,
		InferenceLatencyMs: 45.2,
		LocalQueueDepth:    12,
		ModelServerHealthy: true,
		SyntheticFailure:   "normal",
		CorrelationId:      "corr-42",
	}

	sample := telemetryFromProto(msg)

	if sample.WorkerID != "worker-1" {
		t.Errorf("WorkerID: got %s, want worker-1", sample.WorkerID)
	}
	if !sample.Timestamp.Equal(ts) {
		t.Errorf("Timestamp: got %v, want %v", sample.Timestamp, ts)
	}
	if sample.GPUUtilization != 0.75 {
		t.Errorf("GPUUtilization: got %f, want 0.75", sample.GPUUtilization)
	}
	if sample.VRAMUsedBytes != 8000000000 {
		t.Errorf("VRAMUsedBytes: got %d, want 8000000000", sample.VRAMUsedBytes)
	}
	if sample.VRAMTotalBytes != 16000000000 {
		t.Errorf("VRAMTotalBytes: got %d, want 16000000000", sample.VRAMTotalBytes)
	}
	if sample.TemperatureCelsius != 72.5 {
		t.Errorf("TemperatureCelsius: got %f, want 72.5", sample.TemperatureCelsius)
	}
	if sample.PowerWatts != 250.0 {
		t.Errorf("PowerWatts: got %f, want 250.0", sample.PowerWatts)
	}
	if sample.ECCErrorCount != 3 {
		t.Errorf("ECCErrorCount: got %d, want 3", sample.ECCErrorCount)
	}
	if sample.InferenceLatencyMS != 45.2 {
		t.Errorf("InferenceLatencyMS: got %f, want 45.2", sample.InferenceLatencyMS)
	}
	if sample.LocalQueueDepth != 12 {
		t.Errorf("LocalQueueDepth: got %d, want 12", sample.LocalQueueDepth)
	}
	if !sample.ModelServerHealthy {
		t.Error("ModelServerHealthy: got false, want true")
	}
	if sample.SyntheticFailureFlag != "normal" {
		t.Errorf("SyntheticFailureFlag: got %s, want normal", sample.SyntheticFailureFlag)
	}
	if sample.CorrelationID != "corr-42" {
		t.Errorf("CorrelationID: got %s, want corr-42", sample.CorrelationID)
	}
}

func TestTelemetryFromProtoInvalidTimestampUsesNow(t *testing.T) {
	msg := &aegisv1.AgentTelemetry{
		WorkerId:           "worker-1",
		Timestamp:          "not-a-timestamp",
		ModelServerHealthy: true,
	}
	before := time.Now().Add(-time.Second)
	sample := telemetryFromProto(msg)
	after := time.Now().Add(time.Second)

	if sample.Timestamp.Before(before) || sample.Timestamp.After(after) {
		t.Errorf("expected timestamp near now, got %v", sample.Timestamp)
	}
}

func TestDirectiveToProto(t *testing.T) {
	d := Directive{
		Type:          "redirect",
		OwnerHint:     "cp-b:50051",
		Message:       "owned by another CP",
		CorrelationID: "corr-99",
	}
	pb := directiveToProto(d)
	if pb.DirectiveType != "redirect" {
		t.Errorf("DirectiveType: got %s, want redirect", pb.DirectiveType)
	}
	if pb.OwnerHint != "cp-b:50051" {
		t.Errorf("OwnerHint: got %s, want cp-b:50051", pb.OwnerHint)
	}
	if pb.Message != "owned by another CP" {
		t.Errorf("Message: got %s", pb.Message)
	}
	if pb.CorrelationId != "corr-99" {
		t.Errorf("CorrelationId: got %s, want corr-99", pb.CorrelationId)
	}
}

func TestAddAndEvaluateClearsWindow(t *testing.T) {
	store := testutil.NewMockStore()
	manager := incident.NewManager(store, &testutil.MockPublisher{}, okDiagnostics{}, "cp-a", time.Minute)
	cp := New("cp-a", "a:50051", staticRing{member: hashring.Member{ID: "cp-a", Address: "a:50051"}}, manager, 10, time.Second)
	
	workerID := "worker-test-clear"
	
	// Add 3 high temp samples to trigger overheat detection (SustainedSampleCount is 3 by default).
	for i := 0; i < 3; i++ {
		sample := state.TelemetrySample{WorkerID: workerID, Timestamp: time.Now(), ReceivedAt: time.Now(), TemperatureCelsius: 90, ModelServerHealthy: true}
		_, detected, _ := cp.addAndEvaluate(sample)
		if i < 2 && detected {
			t.Fatalf("unexpected detection at sample %d", i)
		}
		if i == 2 && !detected {
			t.Fatal("expected overheat detection at sample 2")
		}
	}

	// Because sample 2 triggered detection, the sliding window must be explicitly cleared.
	// If it is cleared correctly, the next high temp sample will be treated as the FIRST sample of a new window,
	// and will therefore NOT trigger an immediate detection.
	sample := state.TelemetrySample{WorkerID: workerID, Timestamp: time.Now(), ReceivedAt: time.Now(), TemperatureCelsius: 90, ModelServerHealthy: true}
	_, detected, _ := cp.addAndEvaluate(sample)
	if detected {
		t.Fatal("window was not cleared: 4th sample triggered detection immediately instead of waiting for a new sustained period")
	}
}
