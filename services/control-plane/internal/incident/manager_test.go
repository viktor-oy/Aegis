package incident

import (
	"context"
	"errors"
	"github.com/aegis/aegis/services/control-plane/internal/testutil"
	"testing"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type failingDiagnostics struct{}

func (failingDiagnostics) TriggerDiagnostics(context.Context, state.DiagnosticRequest) (state.DiagnosticBundle, error) {
	return state.DiagnosticBundle{}, errors.New("agent not reachable")
}

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

func TestDeterministicIDStableInBucket(t *testing.T) {
	ts := time.Date(2026, 3, 4, 9, 12, 33, 0, time.UTC)
	left := DeterministicID("worker-a", state.FailureECCBurst, ts, time.Minute)
	right := DeterministicID("worker-a", state.FailureECCBurst, ts.Add(20*time.Second), time.Minute)
	if left != right {
		t.Fatalf("incident ID should be stable within bucket: %s != %s", left, right)
	}
}

func TestDeterministicIDDiffersAcrossBuckets(t *testing.T) {
	ts1 := time.Date(2026, 3, 4, 9, 12, 0, 0, time.UTC)
	ts2 := time.Date(2026, 3, 4, 9, 13, 0, 0, time.UTC) // next minute
	left := DeterministicID("worker-a", state.FailureECCBurst, ts1, time.Minute)
	right := DeterministicID("worker-a", state.FailureECCBurst, ts2, time.Minute)
	if left == right {
		t.Fatalf("incident ID should differ across buckets: %s == %s", left, right)
	}
}

func TestDeterministicIDDiffersForDifferentWorkers(t *testing.T) {
	ts := time.Date(2026, 3, 4, 9, 12, 0, 0, time.UTC)
	left := DeterministicID("worker-a", state.FailureECCBurst, ts, time.Minute)
	right := DeterministicID("worker-b", state.FailureECCBurst, ts, time.Minute)
	if left == right {
		t.Fatal("incident ID should differ for different workers")
	}
}

func TestDeterministicIDDiffersForDifferentFailureTypes(t *testing.T) {
	ts := time.Date(2026, 3, 4, 9, 12, 0, 0, time.UTC)
	left := DeterministicID("worker-a", state.FailureECCBurst, ts, time.Minute)
	right := DeterministicID("worker-a", state.FailureGPUOverheat, ts, time.Minute)
	if left == right {
		t.Fatal("incident ID should differ for different failure types")
	}
}

func TestDeterministicIDHasIncPrefix(t *testing.T) {
	ts := time.Date(2026, 3, 4, 9, 12, 0, 0, time.UTC)
	id := DeterministicID("worker-a", state.FailureECCBurst, ts, time.Minute)
	if len(id) < 4 || id[:4] != "inc_" {
		t.Fatalf("incident ID should start with 'inc_', got %s", id)
	}
}

func TestHandleDetectionPublishesDegradedDiagnostics(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	manager := NewManager(store, pub, failingDiagnostics{}, "cp-test")
	ts := time.Date(2026, 3, 10, 14, 1, 0, 0, time.UTC)
	inc, opened, err := manager.HandleDetection(context.Background(), state.DetectionResult{
		WorkerID:      "worker-a",
		FailureType:   state.FailureMissedHeartbeat,
		Severity:      state.SeverityCritical,
		Reason:        "deadline expired",
		ObservedAt:    ts,
		CorrelationID: "corr-1",
	})
	if err != nil || !opened {
		t.Fatalf("expected opened incident, opened=%v err=%v", opened, err)
	}
	if inc.State != state.WorkerPostmortemRequested {
		t.Fatalf("unexpected final state %s", inc.State)
	}
	if got := len(pub.Events()); got != 4 {
		t.Fatalf("expected 4 events, got %d", got)
	}
}

func TestHandleDetectionWithSuccessfulDiagnostics(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	manager := NewManager(store, pub, okDiagnostics{}, "cp-test")
	ts := time.Date(2026, 3, 10, 14, 1, 0, 0, time.UTC)
	inc, opened, err := manager.HandleDetection(context.Background(), state.DetectionResult{
		WorkerID:      "worker-a",
		FailureType:   state.FailureGPUOverheat,
		Severity:      state.SeverityCritical,
		Reason:        "sustained overheat",
		ObservedAt:    ts,
		CorrelationID: "corr-ok",
	})
	if err != nil || !opened {
		t.Fatalf("expected opened incident, opened=%v err=%v", opened, err)
	}
	if inc.State != state.WorkerPostmortemRequested {
		t.Fatalf("unexpected final state %s", inc.State)
	}

	events := pub.Events()
	if got := len(events); got != 4 {
		t.Fatalf("expected 4 events, got %d", got)
	}

	// Verify correct topic ordering.
	expectedTopics := []string{
		TopicIncidentDetected,
		TopicDiagnosticsRequested,
		TopicDiagnosticsCollected,
		TopicPostmortemRequested,
	}
	for i, expected := range expectedTopics {
		if events[i].Topic != expected {
			t.Errorf("event[%d]: expected topic %s, got %s", i, expected, events[i].Topic)
		}
	}

	// Verify the correlation_id is preserved across all events.
	for i, ev := range events {
		if ev.Envelope.CorrelationID != "corr-ok" {
			t.Errorf("event[%d]: expected correlation_id=corr-ok, got %s", i, ev.Envelope.CorrelationID)
		}
	}
}

func TestValidTransitionMatrix(t *testing.T) {
	cases := []struct {
		from  state.WorkerHealthState
		to    state.WorkerHealthState
		valid bool
	}{
		{state.WorkerHealthy, state.WorkerSuspected, true},
		{state.WorkerSuspected, state.WorkerDiagnosticsTriggered, true},
		{state.WorkerSuspected, state.WorkerResolved, true},
		{state.WorkerDiagnosticsTriggered, state.WorkerDiagnosticsCollected, true},
		{state.WorkerDiagnosticsCollected, state.WorkerPostmortemRequested, true},
		{state.WorkerPostmortemRequested, state.WorkerPostmortemGenerated, true},
		{state.WorkerPostmortemGenerated, state.WorkerDeliveryInProgress, true},
		{state.WorkerDeliveryInProgress, state.WorkerDelivered, true},
		{state.WorkerDeliveryInProgress, state.WorkerDeliveryFailed, true},
		{state.WorkerDelivered, state.WorkerResolved, true},
		{state.WorkerDeliveryFailed, state.WorkerResolved, true},
		// Invalid transitions.
		{state.WorkerHealthy, state.WorkerDelivered, false},
		{state.WorkerSuspected, state.WorkerDelivered, false},
		{state.WorkerResolved, state.WorkerHealthy, false},
		{state.WorkerDelivered, state.WorkerDeliveryFailed, false},
	}
	for _, tc := range cases {
		result := ValidTransition(tc.from, tc.to)
		if result != tc.valid {
			t.Errorf("ValidTransition(%s, %s) = %v, want %v", tc.from, tc.to, result, tc.valid)
		}
	}
}

func TestHandleDetection_IllegalTransitionRoutesToDLQ(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	manager := NewManager(store, pub, okDiagnostics{}, "cp-test")
	ctx := context.Background()

	// Seed existing state as DIAGNOSTICS_TRIGGERED for worker-x:ECCBurst
	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-x",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc_existing_999",
		CurrentState: string(state.WorkerDiagnosticsTriggered),
	})

	// Try to detect again for worker-x:ECCBurst -> should attempt transition DIAGNOSTICS_TRIGGERED -> SUSPECTED, which is illegal
	_, opened, err := manager.HandleDetection(ctx, state.DetectionResult{
		WorkerID:      "worker-x",
		FailureType:   state.FailureECCBurst,
		Severity:      state.SeverityCritical,
		Reason:        "ecc burst again",
		ObservedAt:    time.Now().UTC(),
		CorrelationID: "corr-ill",
	})

	if err == nil || opened {
		t.Fatalf("expected illegal FSM transition error, got opened=%v err=%v", opened, err)
	}

	// Verify DLQ marker set in store
	marker, _ := store.GetDLQMarker(ctx, "worker-x", string(state.FailureECCBurst))
	if marker == "" {
		t.Fatalf("expected DLQ marker to be set in Redis for worker-x:ECCBurst")
	}

	// Verify existing state remained untouched
	existing, _ := store.GetWorkerState(ctx, "worker-x", string(state.FailureECCBurst))
	if existing.CurrentState != string(state.WorkerDiagnosticsTriggered) {
		t.Fatalf("expected existing state to remain DIAGNOSTICS_TRIGGERED, got %s", existing.CurrentState)
	}

	// Verify event published to DLQ topic
	events := pub.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event in DLQ, got %d", len(events))
	}
	if events[0].Topic != TopicCorruptFSMDLQ {
		t.Fatalf("expected topic %s, got %s", TopicCorruptFSMDLQ, events[0].Topic)
	}
}

func TestHandleDetection_MissedHeartbeatReason(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	manager := NewManager(store, pub, okDiagnostics{}, "cp-test")
	ctx := context.Background()

	_, opened, err := manager.HandleDetection(ctx, state.DetectionResult{
		WorkerID:      "worker-hb-reason",
		FailureType:   state.FailureMissedHeartbeat,
		Severity:      state.SeverityCritical,
		Reason:        "deadline expired",
		ObservedAt:    time.Now().UTC(),
		CorrelationID: "corr-hb",
	})

	if err != nil || !opened {
		t.Fatalf("expected detection to succeed, got opened=%v err=%v", opened, err)
	}

	events := pub.Events()
	if len(events) < 1 {
		t.Fatalf("expected events published")
	}

	detectedEvent := events[0]
	if detectedEvent.Topic != TopicIncidentDetected {
		t.Fatalf("expected first event to be %s, got %s", TopicIncidentDetected, detectedEvent.Topic)
	}

	reason, ok := detectedEvent.Envelope.Payload["reason"].(string)
	if !ok || reason != "Node failure suspected due to missed heartbeat min-heap expiry" {
		t.Fatalf("unexpected reason in payload: %v", detectedEvent.Envelope.Payload["reason"])
	}
}
