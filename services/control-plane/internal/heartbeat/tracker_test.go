package heartbeat

import (
	"testing"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/state"
)

func TestTrackerSkipsStaleDeadlines(t *testing.T) {
	tracker := NewTracker(10 * time.Second)
	start := time.Date(2026, 1, 6, 10, 0, 0, 0, time.UTC)
	tracker.Observe("worker-a", start)
	tracker.Observe("worker-a", start.Add(5*time.Second))

	expired := tracker.Expired(start.Add(11 * time.Second))
	if len(expired) != 0 {
		t.Fatalf("stale deadline should be skipped, got %d expired", len(expired))
	}

	expired = tracker.Expired(start.Add(16 * time.Second))
	if len(expired) != 1 || expired[0].WorkerID != "worker-a" {
		t.Fatalf("expected worker-a to expire once, got %#v", expired)
	}
}

func TestTrackerMultipleWorkersIndependent(t *testing.T) {
	tracker := NewTracker(5 * time.Second)
	start := time.Date(2026, 1, 6, 10, 0, 0, 0, time.UTC)
	tracker.Observe("worker-a", start)
	tracker.Observe("worker-b", start.Add(2*time.Second))

	// At start+6s: worker-a expired (deadline was start+5s), worker-b still alive (deadline start+7s).
	expired := tracker.Expired(start.Add(6 * time.Second))
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}
	if expired[0].WorkerID != "worker-a" {
		t.Fatalf("expected worker-a, got %s", expired[0].WorkerID)
	}

	// At start+8s: worker-b also expired.
	expired = tracker.Expired(start.Add(8 * time.Second))
	if len(expired) != 1 || expired[0].WorkerID != "worker-b" {
		t.Fatalf("expected worker-b, got %#v", expired)
	}
}

func TestTrackerNextDeadlineReturnsSoonest(t *testing.T) {
	tracker := NewTracker(10 * time.Second)
	start := time.Date(2026, 1, 6, 10, 0, 0, 0, time.UTC)
	tracker.Observe("worker-b", start.Add(5*time.Second))
	tracker.Observe("worker-a", start) // earlier deadline

	next, ok := tracker.NextDeadline()
	if !ok {
		t.Fatal("expected a next deadline")
	}
	if next.WorkerID != "worker-a" {
		t.Fatalf("expected worker-a as soonest, got %s", next.WorkerID)
	}
}

func TestTrackerSnapshotReturnsCurrentState(t *testing.T) {
	tracker := NewTracker(10 * time.Second)
	start := time.Date(2026, 1, 6, 10, 0, 0, 0, time.UTC)
	tracker.Observe("worker-a", start)

	ws, ok := tracker.Snapshot("worker-a")
	if !ok {
		t.Fatal("expected snapshot for worker-a")
	}
	if ws.WorkerID != "worker-a" {
		t.Fatalf("expected worker-a, got %s", ws.WorkerID)
	}
	if ws.State != state.WorkerHealthy {
		t.Fatalf("expected HEALTHY state, got %s", ws.State)
	}
	if ws.Generation != 1 {
		t.Fatalf("expected generation 1, got %d", ws.Generation)
	}

	// Observe again bumps generation.
	tracker.Observe("worker-a", start.Add(time.Second))
	ws, _ = tracker.Snapshot("worker-a")
	if ws.Generation != 2 {
		t.Fatalf("expected generation 2, got %d", ws.Generation)
	}
}

func TestTrackerSnapshotMissingWorker(t *testing.T) {
	tracker := NewTracker(10 * time.Second)
	_, ok := tracker.Snapshot("nonexistent")
	if ok {
		t.Fatal("expected false for nonexistent worker")
	}
}

func TestTrackerExpiredSetsStateSuspected(t *testing.T) {
	tracker := NewTracker(5 * time.Second)
	start := time.Date(2026, 1, 6, 10, 0, 0, 0, time.UTC)
	tracker.Observe("worker-a", start)

	expired := tracker.Expired(start.Add(6 * time.Second))
	if len(expired) != 1 {
		t.Fatalf("expected 1 expired, got %d", len(expired))
	}
	if expired[0].State != state.WorkerSuspected {
		t.Fatalf("expected SUSPECTED state, got %s", expired[0].State)
	}
}
