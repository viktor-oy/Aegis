package incident

import (
	"context"
	"encoding/json"
	"github.com/aegis/aegis/services/control-plane/internal/testutil"
	"testing"

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
	if st == nil || st.CurrentState != string(state.WorkerResolved) {
		t.Fatalf("expected state to be RESOLVED, got %+v", st)
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
	if st == nil || st.CurrentState != string(state.WorkerResolved) {
		t.Fatalf("expected state to be RESOLVED after dlq terminal delivery, got %+v", st)
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
