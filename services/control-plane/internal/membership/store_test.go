package membership_test

import (
	"context"
	"github.com/aegis/aegis/services/control-plane/internal/testutil"
	"testing"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/membership"
)

func TestWorkerStateLifecycle(t *testing.T) {
	store := testutil.NewMockStore()
	ctx := context.Background()

	state1 := membership.WorkerState{
		WorkerID:      "worker-1",
		ErrorType:     "SustainedVRAMPressure",
		IncidentID:    "inc-111",
		CurrentState:  "SUSPECTED",
		CorrelationID: "corr-111",
		UpdatedAt:     time.Now().UTC(),
	}

	state2 := membership.WorkerState{
		WorkerID:      "worker-1",
		ErrorType:     "MissedHeartbeat",
		IncidentID:    "inc-222",
		CurrentState:  "DIAGNOSTICS_TRIGGERED",
		CorrelationID: "corr-222",
		UpdatedAt:     time.Now().UTC(),
	}

	if err := store.SetWorkerState(ctx, state1); err != nil {
		t.Fatalf("SetWorkerState failed: %v", err)
	}
	if err := store.SetWorkerState(ctx, state2); err != nil {
		t.Fatalf("SetWorkerState failed: %v", err)
	}

	got1, err := store.GetWorkerState(ctx, "worker-1", "SustainedVRAMPressure")
	if err != nil || got1 == nil {
		t.Fatalf("expected state1, got nil (err=%v)", err)
	}
	if got1.IncidentID != "inc-111" || got1.CurrentState != "SUSPECTED" {
		t.Fatalf("unexpected state1 content: %+v", got1)
	}

	got2, err := store.GetWorkerState(ctx, "worker-1", "MissedHeartbeat")
	if err != nil || got2 == nil {
		t.Fatalf("expected state2, got nil (err=%v)", err)
	}
	if got2.IncidentID != "inc-222" {
		t.Fatalf("unexpected state2 incidentID: %s", got2.IncidentID)
	}

	list, err := store.ListActiveWorkerStates(ctx)
	if err != nil {
		t.Fatalf("ListActiveWorkerStates failed: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 active worker states, got %d", len(list))
	}

	if err := store.DeleteWorkerState(ctx, "worker-1", "SustainedVRAMPressure"); err != nil {
		t.Fatalf("DeleteWorkerState failed: %v", err)
	}

	gotDeleted, err := store.GetWorkerState(ctx, "worker-1", "SustainedVRAMPressure")
	if err != nil || gotDeleted != nil {
		t.Fatalf("expected nil after delete, got %+v (err=%v)", gotDeleted, err)
	}
}

func TestDLQMarkerLifecycle(t *testing.T) {
	store := testutil.NewMockStore()
	ctx := context.Background()

	if err := store.SetDLQMarker(ctx, "worker-99", "ECCBurst", "CORRUPTED_FSM"); err != nil {
		t.Fatalf("SetDLQMarker failed: %v", err)
	}

	val, err := store.GetDLQMarker(ctx, "worker-99", "ECCBurst")
	if err != nil || val != "CORRUPTED_FSM" {
		t.Fatalf("expected 'CORRUPTED_FSM', got '%s' (err=%v)", val, err)
	}

	markers, err := store.ListDLQMarkers(ctx)
	if err != nil {
		t.Fatalf("ListDLQMarkers failed: %v", err)
	}
	if len(markers) != 1 || markers[0] != "aegis:cp:dlq:corrupt:worker-99:ECCBurst" {
		t.Fatalf("unexpected DLQ markers list: %v", markers)
	}

	if err := store.DeleteDLQMarker(ctx, "worker-99", "ECCBurst"); err != nil {
		t.Fatalf("DeleteDLQMarker failed: %v", err)
	}

	valAfter, err := store.GetDLQMarker(ctx, "worker-99", "ECCBurst")
	if err != nil || valAfter != "" {
		t.Fatalf("expected empty string after delete, got '%s'", valAfter)
	}
}

func TestReleaseIncidentLock(t *testing.T) {
	store := testutil.NewMockStore()
	ctx := context.Background()
	now := time.Now()

	ok, err := store.AcquireIncidentLock(ctx, "worker-10", "inc-999", 10*time.Second, now)
	if err != nil || !ok {
		t.Fatalf("failed to acquire incident lock")
	}

	// Should not be able to acquire with a different incident ID
	ok, err = store.AcquireIncidentLock(ctx, "worker-10", "inc-other", 10*time.Second, now)
	if err != nil || ok {
		t.Fatalf("expected lock acquisition to fail for different incidentID")
	}

	// Release with matching incidentID
	if err := store.ReleaseIncidentLock(ctx, "worker-10", "inc-999"); err != nil {
		t.Fatalf("ReleaseIncidentLock failed: %v", err)
	}

	// Should now succeed with new incident ID
	ok, err = store.AcquireIncidentLock(ctx, "worker-10", "inc-other", 10*time.Second, now)
	if err != nil || !ok {
		t.Fatalf("expected lock acquisition to succeed after release")
	}
}
