package heartbeat

import (
	"testing"
	"time"
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

