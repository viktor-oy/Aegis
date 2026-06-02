package incident

import (
	"context"
	"encoding/json"
	"github.com/aegis/aegis/services/control-plane/internal/testutil"
	"testing"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

func TestFSMConsumer_PostmortemGenerated(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	consumer := NewFSMConsumer(store, pub, "cp-test")
	ctx := context.Background()

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-1",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc_pm_1",
		CurrentState: string(state.WorkerPostmortemRequested),
	})

	env := state.EventEnvelope{
		EventType:  TopicPostmortemGenerated,
		IncidentID: "inc_pm_1",
		WorkerID:   "worker-1",
		Payload: map[string]any{
			"failure_type": string(state.FailureECCBurst),
		},
	}
	data, _ := json.Marshal(env)

	if err := consumer.ConsumeEvent(ctx, TopicPostmortemGenerated, data); err != nil {
		t.Fatalf("ConsumeEvent failed: %v", err)
	}

	st, _ := store.GetWorkerState(ctx, "worker-1", string(state.FailureECCBurst))
	if st == nil || st.CurrentState != string(state.WorkerDeliveryInProgress) {
		t.Fatalf("expected state to be DELIVERY_IN_PROGRESS, got %+v", st)
	}
	if store.AcquireLockCalls() == 0 {
		t.Fatalf("expected AcquireFSMLock to be called")
	}
}

func TestFSMConsumer_DeliveryDelivered(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	consumer := NewFSMConsumer(store, pub, "cp-test")
	ctx := context.Background()

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-2",
		ErrorType:    string(state.FailureGPUOverheat),
		IncidentID:   "inc_deliv_2",
		CurrentState: string(state.WorkerDeliveryInProgress),
	})

	payload := map[string]any{
		"incident_id": "inc_deliv_2",
		"sink":        "email",
		"status":      "delivered",
	}
	data, _ := json.Marshal(payload)

	if err := consumer.ConsumeEvent(ctx, TopicDeliveryStatus, data); err != nil {
		t.Fatalf("ConsumeEvent failed: %v", err)
	}

	st, _ := store.GetWorkerState(ctx, "worker-2", string(state.FailureGPUOverheat))
	if st == nil || st.CurrentState != string(state.WorkerDelivered) {
		t.Fatalf("expected state to be DELIVERED, got %+v", st)
	}
	if store.AcquireLockCalls() == 0 {
		t.Fatalf("expected AcquireFSMLock to be called")
	}
}

func TestFSMConsumer_DeliveryDLQ(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	consumer := NewFSMConsumer(store, pub, "cp-test")
	ctx := context.Background()

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-3",
		ErrorType:    string(state.FailureVRAMPressure),
		IncidentID:   "inc_dlq_3",
		CurrentState: string(state.WorkerDeliveryInProgress),
	})

	payload := map[string]any{
		"incident_id": "inc_dlq_3",
		"sink":        "file",
		"status":      "dlq",
		"error":       "disk full",
	}
	data, _ := json.Marshal(payload)

	if err := consumer.ConsumeEvent(ctx, TopicDeliveryDLQ, data); err != nil {
		t.Fatalf("ConsumeEvent failed: %v", err)
	}

	st, _ := store.GetWorkerState(ctx, "worker-3", string(state.FailureVRAMPressure))
	if st == nil || st.CurrentState != string(state.WorkerDeliveryFailed) {
		t.Fatalf("expected state to be DELIVERY_FAILED after dlq terminal delivery, got %+v", st)
	}
	if store.AcquireLockCalls() == 0 {
		t.Fatalf("expected AcquireFSMLock to be called")
	}
}

func TestFSMConsumer_IllegalTransitionToDLQ(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	consumer := NewFSMConsumer(store, pub, "cp-test")
	ctx := context.Background()

	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-4",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc_ill_4",
		CurrentState: string(state.WorkerSuspected),
	})

	payload := map[string]any{
		"incident_id": "inc_ill_4",
		"sink":        "email",
		"status":      "delivered",
	}
	data, _ := json.Marshal(payload)

	err := consumer.ConsumeEvent(ctx, TopicDeliveryStatus, data)
	if err == nil {
		t.Fatalf("expected illegal transition error when jumping SUSPECTED -> DELIVERED")
	}

	st, _ := store.GetWorkerState(ctx, "worker-4", string(state.FailureECCBurst))
	if st == nil || st.CurrentState != string(state.WorkerSuspected) {
		t.Fatalf("expected state to remain SUSPECTED untouched, got %+v", st)
	}

	events := pub.Events()
	if len(events) != 1 || events[0].Topic != TopicCorruptFSMDLQ {
		t.Fatalf("expected 1 event published to DLQ, got %v", events)
	}
}

func TestFSMConsumer_CorruptMarkerRejectsValidTransitions(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	consumer := NewFSMConsumer(store, pub, "cp-test")
	ctx := context.Background()

	// Inject a DLQ marker signifying this FSM is already corrupt
	_ = store.SetDLQMarker(ctx, "worker-corrupt-1", string(state.FailureLatencySpike), "illegal transition previously occurred")

	// Set current state to POSTMORTEM_REQUESTED
	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-corrupt-1",
		ErrorType:    string(state.FailureLatencySpike),
		IncidentID:   "inc_corrupt_1",
		CurrentState: string(state.WorkerPostmortemRequested),
	})

	// Create a normally VALID transition event: PostmortemRequested -> PostmortemGenerated
	env := state.EventEnvelope{
		EventType:     TopicPostmortemGenerated,
		IncidentID:    "inc_corrupt_1",
		WorkerID:      "worker-corrupt-1",
		CorrelationID: "corr-123",
		Payload: map[string]any{
			"failure_type": string(state.FailureLatencySpike),
		},
	}
	data, _ := json.Marshal(env)

	err := consumer.ConsumeEvent(ctx, TopicPostmortemGenerated, data)
	if err != nil {
		t.Fatalf("expected nil error (graceful drop) when processing corrupt FSM, got: %v", err)
	}

	// Verify state was NOT updated (should still be PostmortemRequested, NOT Generated)
	st, _ := store.GetWorkerState(ctx, "worker-corrupt-1", string(state.FailureLatencySpike))
	if st.CurrentState != string(state.WorkerPostmortemRequested) {
		t.Fatalf("expected state to remain frozen at %s, got %s", state.WorkerPostmortemRequested, st.CurrentState)
	}
}

func TestFSMConsumer_DeferredDelivery_MatchesIncident(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	consumer := NewFSMConsumer(store, pub, "cp-test")
	ctx := context.Background()

	// Setup state
	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-def-1",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc_def_1",
		CurrentState: string(state.WorkerPostmortemRequested),
	})

	// Inject a deferred DELIVERED event matching this incident
	defPayload := map[string]any{
		"incident_id":    "inc_def_1",
		"status":         "delivered",
		"correlation_id": "corr-def",
	}
	defData, _ := json.Marshal(defPayload)
	_ = store.DeferEvent(ctx, "worker-def-1", string(state.FailureECCBurst), TopicDeliveryStatus, defData, 5*time.Minute)

	// Consume POSTMORTEM_GENERATED (which triggers the check for deferred delivery)
	env := state.EventEnvelope{
		EventType:     TopicPostmortemGenerated,
		IncidentID:    "inc_def_1",
		WorkerID:      "worker-def-1",
		CorrelationID: "corr-123",
		Payload: map[string]any{
			"failure_type": string(state.FailureECCBurst),
		},
	}
	data, _ := json.Marshal(env)

	if err := consumer.ConsumeEvent(ctx, TopicPostmortemGenerated, data); err != nil {
		t.Fatalf("ConsumeEvent failed: %v", err)
	}

	// Should have fast-forwarded to DELIVERED using the deferred event
	st, _ := store.GetWorkerState(ctx, "worker-def-1", string(state.FailureECCBurst))
	if st.CurrentState != string(state.WorkerDelivered) {
		t.Fatalf("expected state to fast-forward to DELIVERED, got %s", st.CurrentState)
	}
	if store.AcquireLockCalls() == 0 {
		t.Fatalf("expected AcquireFSMLock to be called")
	}
}

func TestFSMConsumer_DeferredDelivery_IgnoresStale(t *testing.T) {
	store := testutil.NewMockStore()
	pub := &testutil.MockPublisher{}
	consumer := NewFSMConsumer(store, pub, "cp-test")
	ctx := context.Background()

	// Setup state
	_ = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:     "worker-def-2",
		ErrorType:    string(state.FailureECCBurst),
		IncidentID:   "inc_def_2_NEW",
		CurrentState: string(state.WorkerPostmortemRequested),
	})

	// Inject a deferred DELIVERED event from an OLD incident
	defPayload := map[string]any{
		"incident_id":    "inc_def_OLD",
		"status":         "delivered",
		"correlation_id": "corr-def-old",
	}
	defData, _ := json.Marshal(defPayload)
	_ = store.DeferEvent(ctx, "worker-def-2", string(state.FailureECCBurst), TopicDeliveryStatus, defData, 5*time.Minute)

	// Consume POSTMORTEM_GENERATED for the NEW incident
	env := state.EventEnvelope{
		EventType:     TopicPostmortemGenerated,
		IncidentID:    "inc_def_2_NEW",
		WorkerID:      "worker-def-2",
		CorrelationID: "corr-new",
		Payload: map[string]any{
			"failure_type": string(state.FailureECCBurst),
		},
	}
	data, _ := json.Marshal(env)

	if err := consumer.ConsumeEvent(ctx, TopicPostmortemGenerated, data); err != nil {
		t.Fatalf("ConsumeEvent failed: %v", err)
	}

	// Should ONLY transition to DELIVERY_IN_PROGRESS, NOT fast-forward, because the deferred event is stale
	st, _ := store.GetWorkerState(ctx, "worker-def-2", string(state.FailureECCBurst))
	if st.CurrentState != string(state.WorkerDeliveryInProgress) {
		t.Fatalf("expected state to only progress to DELIVERY_IN_PROGRESS, got %s", st.CurrentState)
	}
	if store.AcquireLockCalls() == 0 {
		t.Fatalf("expected AcquireFSMLock to be called")
	}
}
