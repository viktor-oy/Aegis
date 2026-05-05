package main

import (
	"context"
	"github.com/aegis/aegis/services/control-plane/internal/testutil"
	"testing"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

func TestRunResolve_DefaultResolve(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	ctx := context.Background()
	now := time.Now().UTC()

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-10",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc-100",
		CurrentState: string(state.WorkerDelivered),
	})
	_ = store.SetDLQMarker(ctx, "worker-10", string(state.FailureECCBurst), "illegal transition")

	err := RunResolve(ctx, store, pub, "worker-10", string(state.FailureECCBurst), false, false, "", now)
	if err != nil {
		t.Fatalf("RunResolve failed: %v", err)
	}

	st, _ := store.GetWorkerState(ctx, "worker-10", string(state.FailureECCBurst))
	if st == nil || st.CurrentState != string(state.WorkerResolved) {
		t.Fatalf("expected state RESOLVED, got %+v", st)
	}

	m, _ := store.GetDLQMarker(ctx, "worker-10", string(state.FailureECCBurst))
	if m != "" {
		t.Fatalf("expected DLQ marker to be cleared, got %s", m)
	}

	events := pub.Events()
	if len(events) != 1 || events[0].Topic != incident.TopicCorruptFSMDLQ {
		t.Fatalf("expected 1 event on %s, got %v", incident.TopicCorruptFSMDLQ, events)
	}
	if events[0].Envelope.EventType != "aegis.cp.corrupt-fsm.repair" {
		t.Fatalf("unexpected event type: %s", events[0].Envelope.EventType)
	}
}

func TestRunResolve_FixCorruptFSM(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	ctx := context.Background()
	now := time.Now().UTC()

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-20",
		ErrorType:    string(state.FailureGPUOverheat),
		IncidentID:   "inc-200",
		CurrentState: string(state.WorkerSuspected),
	})
	_ = store.SetDLQMarker(ctx, "worker-20", string(state.FailureGPUOverheat), "corrupt fsm marker")

	err := RunResolve(ctx, store, pub, "worker-20", string(state.FailureGPUOverheat), true, false, "", now)
	if err != nil {
		t.Fatalf("RunResolve failed: %v", err)
	}

	st, _ := store.GetWorkerState(ctx, "worker-20", string(state.FailureGPUOverheat))
	if st == nil || st.CurrentState != string(state.WorkerHealthy) {
		t.Fatalf("expected state HEALTHY with --fix-corrupt-fsm, got %+v", st)
	}

	m, _ := store.GetDLQMarker(ctx, "worker-20", string(state.FailureGPUOverheat))
	if m != "" {
		t.Fatalf("expected DLQ marker to be cleared")
	}

	events := pub.Events()
	if len(events) != 1 || events[0].Envelope.EventType != "aegis.cp.corrupt-fsm.repair" {
		t.Fatalf("expected repair tombstone event, got %v", events)
	}
}

func TestRunResolve_ForceOverride(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	ctx := context.Background()
	now := time.Now().UTC()

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-30",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc-300",
		CurrentState: string(state.WorkerResolved),
	})

	err := RunResolve(ctx, store, pub, "worker-30", string(state.FailureECCBurst), false, true, "", now)
	if err != nil {
		t.Fatalf("expected --force to succeed, got %v", err)
	}

	st, _ := store.GetWorkerState(ctx, "worker-30", string(state.FailureECCBurst))
	if st == nil || st.CurrentState != string(state.WorkerResolved) {
		t.Fatalf("expected state RESOLVED, got %+v", st)
	}
}

func TestRunResolve_IllegalTransitionWithoutForce(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	ctx := context.Background()
	now := time.Now().UTC()

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-40",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc-400",
		CurrentState: string(state.WorkerPostmortemGenerated),
	})

	err := RunResolve(ctx, store, pub, "worker-40", string(state.FailureECCBurst), false, false, "", now)
	if err == nil {
		t.Fatalf("expected illegal transition error when not using --force or --fix-corrupt-fsm, got nil")
	}
}

func TestRunResolve_FixCorruptFSM_NoExistingState(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	ctx := context.Background()
	now := time.Now().UTC()

	// Do not seed any worker state.

	err := RunResolve(ctx, store, pub, "worker-50", string(state.FailureECCBurst), true, false, "", now)
	if err == nil || err.Error() != "cannot fix corrupt FSM: no existing state found for this worker" {
		t.Fatalf("expected error 'cannot fix corrupt FSM: no existing state found for this worker', got %v", err)
	}
}

func TestRunResolve_ForceOverride_WithState(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	ctx := context.Background()
	now := time.Now().UTC()

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-60",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc-600",
		CurrentState: string(state.WorkerPostmortemGenerated),
	})

	err := RunResolve(ctx, store, pub, "worker-60", string(state.FailureECCBurst), false, true, "SUSPECTED", now)
	if err != nil {
		t.Fatalf("expected --force with --state to succeed, got %v", err)
	}

	st, _ := store.GetWorkerState(ctx, "worker-60", string(state.FailureECCBurst))
	if st == nil || st.CurrentState != string(state.WorkerSuspected) {
		t.Fatalf("expected state SUSPECTED, got %+v", st)
	}
}
