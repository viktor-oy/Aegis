package incident

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

const (
	TopicPostmortemGenerated = "aegis.postmortem.generated"
	TopicDeliveryStatus      = "aegis.postmortem.delivery.status"
	TopicDeliveryDLQ         = "aegis.postmortem.delivery.dlq"
)

type FSMConsumer struct {
	store     Store
	publisher kafka.Publisher
	producer  string
}

func NewFSMConsumer(store Store, publisher kafka.Publisher, producer string) *FSMConsumer {
	return &FSMConsumer{
		store:     store,
		publisher: publisher,
		producer:  producer,
	}
}

// ConsumeEvent parses a Kafka message payload from downstream topics and updates Redis FSM state.
func (c *FSMConsumer) ConsumeEvent(ctx context.Context, topic string, value []byte) error {
	if len(value) == 0 {
		return nil
	}

	switch topic {
	case TopicPostmortemGenerated:
		return c.handlePostmortemGenerated(ctx, value)
	case TopicDeliveryStatus, TopicDeliveryDLQ:
		return c.handleDeliveryStatus(ctx, topic, value)
	default:
		// Also inspect json envelope EventType in case topic name is generic
		var env state.EventEnvelope
		if err := json.Unmarshal(value, &env); err == nil {
			if env.EventType == TopicPostmortemGenerated {
				return c.handlePostmortemGenerated(ctx, value)
			}
		}
		slog.Debug("fsm_consumer: ignored unhandled topic", "topic", topic)
		return nil
	}
}

func (c *FSMConsumer) handlePostmortemGenerated(ctx context.Context, value []byte) error {
	var env state.EventEnvelope
	if err := json.Unmarshal(value, &env); err != nil {
		return fmt.Errorf("fsm_consumer: unmarshal postmortem generated envelope: %w", err)
	}

	workerID := env.WorkerID
	incidentID := env.IncidentID
	correlationID := env.CorrelationID
	var errorType string

	if ft, ok := env.Payload["failure_type"].(string); ok && ft != "" {
		errorType = ft
	} else if env.CausationID != "" && strings.Contains(env.CausationID, ":") {
		parts := strings.SplitN(env.CausationID, ":", 2)
		workerID = parts[0]
		errorType = parts[1]
	}

	if workerID == "" || errorType == "" {
		states, err := c.store.ListActiveWorkerStates(ctx)
		if err != nil {
			return err
		}
		for _, st := range states {
			if st.IncidentID == incidentID {
				workerID = st.WorkerID
				errorType = st.ErrorType
				break
			}
		}
	}

	if workerID == "" || errorType == "" {
		slog.Warn("fsm_consumer: could not find active worker state for incident", "incident_id", incidentID)
		return nil
	}

	// Transition to POSTMORTEM_GENERATED
	if err := c.transition(ctx, workerID, errorType, incidentID, correlationID, state.WorkerPostmortemGenerated); err != nil {
		return err
	}
	// Immediately transition to DELIVERY_IN_PROGRESS as delivery starts right after postmortem generation
	return c.transition(ctx, workerID, errorType, incidentID, correlationID, state.WorkerDeliveryInProgress)
}

type deliveryResultPayload struct {
	IncidentID    string `json:"incident_id"`
	Sink          string `json:"sink"`
	Status        string `json:"status"`
	CorrelationID string `json:"correlation_id"`
	Error         string `json:"error"`
}

func (c *FSMConsumer) handleDeliveryStatus(ctx context.Context, topic string, value []byte) error {
	var payload deliveryResultPayload
	if err := json.Unmarshal(value, &payload); err != nil {
		// Attempt unmarshaling as envelope
		var env state.EventEnvelope
		if err2 := json.Unmarshal(value, &env); err2 == nil && env.IncidentID != "" {
			payload.IncidentID = env.IncidentID
			payload.CorrelationID = env.CorrelationID
			if st, ok := env.Payload["status"].(string); ok {
				payload.Status = st
			}
		} else {
			return fmt.Errorf("fsm_consumer: unmarshal delivery status: %w", err)
		}
	}

	incidentID := payload.IncidentID
	if incidentID == "" {
		return nil
	}

	var workerID, errorType string
	states, err := c.store.ListActiveWorkerStates(ctx)
	if err != nil {
		return err
	}
	for _, st := range states {
		if st.IncidentID == incidentID {
			workerID = st.WorkerID
			errorType = st.ErrorType
			break
		}
	}

	if workerID == "" || errorType == "" {
		slog.Warn("fsm_consumer: could not find active worker state for delivery result", "incident_id", incidentID, "topic", topic)
		return nil
	}

	isDLQ := topic == TopicDeliveryDLQ || payload.Status == "dlq" || payload.Status == "failed"
	if isDLQ {
		if err := c.transition(ctx, workerID, errorType, incidentID, payload.CorrelationID, state.WorkerDeliveryFailed); err != nil {
			return err
		}
	} else if payload.Status == "delivered" {
		if err := c.transition(ctx, workerID, errorType, incidentID, payload.CorrelationID, state.WorkerDelivered); err != nil {
			return err
		}
	} else {
		slog.Debug("fsm_consumer: ignored non-terminal delivery status", "status", payload.Status)
		return nil
	}

	// Always transition to RESOLVED from DELIVERED or DELIVERY_FAILED
	return c.transition(ctx, workerID, errorType, incidentID, payload.CorrelationID, state.WorkerResolved)
}

func (c *FSMConsumer) transition(ctx context.Context, workerID, errorType, incidentID, correlationID string, toState state.WorkerHealthState) error {
	existing, err := c.store.GetWorkerState(ctx, workerID, errorType)
	fromState := state.WorkerHealthy
	if err == nil && existing != nil && existing.CurrentState != "" {
		fromState = state.WorkerHealthState(existing.CurrentState)
	}

	// If already in target state or already RESOLVED, ignore idempotent retry
	if fromState == toState || fromState == state.WorkerResolved {
		return nil
	}

	if !ValidTransition(fromState, toState) {
		slog.Error("fsm_consumer: illegal FSM transition rejected",
			"metric", "aegis_fsm_illegal_transitions_total",
			"worker_id", workerID,
			"error_type", errorType,
			"incident_id", incidentID,
			"from_state", string(fromState),
			"to_state", string(toState),
			"correlation_id", correlationID,
		)
		errMsg := fmt.Sprintf("illegal FSM transition from %s to %s", fromState, toState)
		_ = c.store.SetDLQMarker(ctx, workerID, errorType, errMsg)
		inc := state.Incident{
			IncidentID:    incidentID,
			WorkerID:      workerID,
			FailureType:   state.FailureType(errorType),
			State:         fromState,
			CorrelationID: correlationID,
		}
		if c.publisher != nil {
			env := kafka.NewEnvelope("aegis.cp.fsm_illegal_transition", inc, c.producer, fmt.Sprintf("%s:%s", workerID, errorType), map[string]any{
				"from_state": string(fromState),
				"to_state":   string(toState),
				"error":      errMsg,
				"metric":     "aegis_fsm_illegal_transitions_total",
			}, time.Now().UTC())
			_ = c.publisher.Publish(ctx, TopicCorruptFSMDLQ, env)
		}
		return fmt.Errorf("illegal FSM transition from %s to %s for worker %s", fromState, toState, workerID)
	}

	return c.store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:      workerID,
		ErrorType:     errorType,
		IncidentID:    incidentID,
		CurrentState:  string(toState),
		CorrelationID: correlationID,
		UpdatedAt:     time.Now().UTC(),
	})
}
