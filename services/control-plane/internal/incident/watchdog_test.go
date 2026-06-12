package incident

import (
	"context"
	"github.com/aegis/aegis/services/control-plane/internal/testutil"
	"os"
	"testing"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type mockRingProvider struct {
	ownerMap map[string]string // workerID -> cpID
}

func (m mockRingProvider) Owner(workerID string) (hashring.Member, bool) {
	ownerID, ok := m.ownerMap[workerID]
	if !ok {
		return hashring.Member{}, false
	}
	return hashring.Member{ID: ownerID}, true
}

func TestWatchdog_Sharding(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	
	// Create watchdog belonging to cp-a
	watchdog := NewWatchdog(store, pub, "cp-test", "cp-a").WithStuckThreshold(15 * time.Minute)
	
	ring := mockRingProvider{
		ownerMap: map[string]string{
			"worker-stuck-a": "cp-a", // belongs to cp-a
			"worker-stuck-b": "cp-b", // belongs to cp-b
			"worker-corrupt-a": "cp-a",
			"worker-corrupt-b": "cp-b",
		},
	}
	watchdog.UpdateRing(ring)
	
	ctx := context.Background()
	now := time.Now().UTC()
	stuckTime := now.Add(-20 * time.Minute)

	// Add 2 stuck states
	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-stuck-a",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc_1",
		CurrentState: string(state.WorkerSuspected),
		UpdatedAt:    stuckTime,
	}, "", 0, 0)
	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-stuck-b",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc_2",
		CurrentState: string(state.WorkerSuspected),
		UpdatedAt:    stuckTime,
	}, "", 0, 0)
	
	// Add 2 corrupt markers
	_ = store.SetDLQMarker(ctx, "worker-corrupt-a", "ECCBurst", "illegal")
	_ = store.SetDLQMarker(ctx, "worker-corrupt-b", "ECCBurst", "illegal")

	stuck, corrupt, err := watchdog.InspectOnce(ctx, now)
	if err != nil {
		t.Fatalf("InspectOnce failed: %v", err)
	}

	// Should only process worker-stuck-a and worker-corrupt-a (owned by cp-a)
	if stuck != 1 {
		t.Fatalf("expected 1 stuck incident processed, got %d", stuck)
	}
	if corrupt != 1 {
		t.Fatalf("expected 1 corrupt marker processed, got %d", corrupt)
	}
	
	events := pub.Events()
	if len(events) != 2 {
		t.Fatalf("expected exactly 2 events published by owner, got %d", len(events))
	}
}

func TestWatchdog_StuckIncidentDetection(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	watchdog := NewWatchdog(store, pub, "cp-test", "cp-a").WithStuckThreshold(15 * time.Minute)
	ctx := context.Background()

	now := time.Now().UTC()
	stuckTime := now.Add(-20 * time.Minute)

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-stuck-1",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc_stuck_100",
		CurrentState: string(state.WorkerSuspected),
		UpdatedAt:    stuckTime,
	}, "", 0, 0)

	stuck, corrupt, err := watchdog.InspectOnce(ctx, now)
	if err != nil {
		t.Fatalf("InspectOnce failed: %v", err)
	}

	if stuck != 1 {
		t.Fatalf("expected 1 stuck incident, got %d", stuck)
	}
	if corrupt != 0 {
		t.Fatalf("expected 0 corrupt markers, got %d", corrupt)
	}

	// Ensure watchdog did NOT auto-resolve or delete the state
	st, _ := store.GetWorkerState(ctx, "worker-stuck-1", string(state.FailureECCBurst))
	if st == nil || st.CurrentState != string(state.WorkerSuspected) {
		t.Fatalf("expected state to remain SUSPECTED untouched by watchdog, got %+v", st)
	}

	events := pub.Events()
	if len(events) != 1 || events[0].Topic != TopicCorruptFSMDLQ {
		t.Fatalf("expected 1 alert published to DLQ topic, got %v", events)
	}
	if events[0].Envelope.EventType != "aegis.cp.fsm_stuck_incident" {
		t.Fatalf("unexpected event type: %s", events[0].Envelope.EventType)
	}
}

func TestWatchdog_NotStuckIncident(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	watchdog := NewWatchdog(store, pub, "cp-test", "cp-a").WithStuckThreshold(15 * time.Minute)
	ctx := context.Background()

	now := time.Now().UTC()
	recentTime := now.Add(-5 * time.Minute)

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-recent-1",
		ErrorType:    string(state.FailureGPUOverheat),
		IncidentID:   "inc_rec_101",
		CurrentState: string(state.WorkerDiagnosticsTriggered),
		UpdatedAt:    recentTime,
	}, "", 0, 0)

	stuck, corrupt, err := watchdog.InspectOnce(ctx, now)
	if err != nil {
		t.Fatalf("InspectOnce failed: %v", err)
	}

	if stuck != 0 || corrupt != 0 {
		t.Fatalf("expected 0 stuck and 0 corrupt, got stuck=%d corrupt=%d", stuck, corrupt)
	}
	if len(pub.Events()) != 0 {
		t.Fatalf("expected no events published for recent incident")
	}
}

func TestWatchdog_CorruptMarkerDetection(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	watchdog := NewWatchdog(store, pub, "cp-test", "cp-a")
	ctx := context.Background()

	_ = store.SetDLQMarker(ctx, "worker-corrupt-1", "ECCBurst", "illegal transition error")

	stuck, corrupt, err := watchdog.InspectOnce(ctx, time.Now().UTC())
	if err != nil {
		t.Fatalf("InspectOnce failed: %v", err)
	}

	if corrupt != 1 || stuck != 0 {
		t.Fatalf("expected corrupt=1 stuck=0, got corrupt=%d stuck=%d", corrupt, stuck)
	}

	// Verify marker remains untouched
	m, _ := store.GetDLQMarker(ctx, "worker-corrupt-1", "ECCBurst")
	if m == "" {
		t.Fatalf("expected marker to remain untouched by watchdog")
	}

	events := pub.Events()
	if len(events) != 1 || events[0].Topic != TopicCorruptFSMDLQ {
		t.Fatalf("expected 1 alert published to DLQ topic, got %v", events)
	}
	if events[0].Envelope.EventType != "aegis.cp.fsm_corrupt_marker" {
		t.Fatalf("unexpected event type: %s", events[0].Envelope.EventType)
	}
}

func TestWatchdog_IntervalConfiguration(t *testing.T) {
	os.Setenv("AEGIS_FSM_WATCHDOG_INTERVAL_SECONDS", "60")
	defer os.Unsetenv("AEGIS_FSM_WATCHDOG_INTERVAL_SECONDS")

	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	watchdog := NewWatchdog(store, pub, "cp-test", "cp-a")

	// Test by inspecting an empty store
	stuck, corrupt, err := watchdog.InspectOnce(context.Background(), time.Now().UTC())
	if err != nil || stuck != 0 || corrupt != 0 {
		t.Fatalf("unexpected InspectOnce result on configured watchdog: err=%v stuck=%d corrupt=%d", err, stuck, corrupt)
	}
}
