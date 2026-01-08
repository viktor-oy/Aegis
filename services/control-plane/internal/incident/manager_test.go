package incident

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type failingDiagnostics struct{}

func (failingDiagnostics) TriggerDiagnostics(context.Context, state.DiagnosticRequest) (state.DiagnosticBundle, error) {
	return state.DiagnosticBundle{}, errors.New("agent not reachable")
}

func TestDeterministicIDStableInBucket(t *testing.T) {
	ts := time.Date(2026, 3, 4, 9, 12, 33, 0, time.UTC)
	left := DeterministicID("worker-a", state.FailureECCBurst, ts, time.Minute)
	right := DeterministicID("worker-a", state.FailureECCBurst, ts.Add(20*time.Second), time.Minute)
	if left != right {
		t.Fatalf("incident ID should be stable within bucket: %s != %s", left, right)
	}
}

func TestHandleDetectionPublishesDegradedDiagnostics(t *testing.T) {
	store := membership.NewInMemoryStore()
	pub := &kafka.MemoryPublisher{}
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

