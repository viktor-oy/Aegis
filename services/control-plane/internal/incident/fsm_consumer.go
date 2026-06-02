package incident

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/kafka"

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
	slog.Info("received event", "component", "FSM_CONSUMER", "event", "KAFKA_RECEIVE", "topic", topic, "payload_len", len(value))
	if len(value) == 0 {
		return nil
	}
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("raw event payload", "component", "FSM_CONSUMER", "event", "KAFKA_RAW", "topic", topic, "payload", string(value))
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
		slog.Debug("ignored unhandled topic", "component", "FSM_CONSUMER", "event", "KAFKA_IGNORED", "topic", topic)
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

	if workerID == "" || errorType == "" || incidentID == "" {
		slog.Warn("could not find active worker state for incident", "component", "FSM_CONSUMER", "event", "STATE_NOT_FOUND", "incident_id", incidentID)
		return nil
	}

	if IsStale(ctx, c.store, workerID, errorType, incidentID) {
		slog.Debug("ignoring stale kafka event for old incident", "component", "FSM_CONSUMER", "worker_id", workerID)
		return nil
	}

	locked, err := c.store.AcquireFSMLock(ctx, workerID, errorType, incidentID, 5*time.Second)
	if err != nil || !locked {
		slog.Debug("could not acquire lock for postmortem generated, skipping", "worker_id", workerID, "error_type", errorType)
		return nil
	}
	defer c.store.ReleaseFSMLock(ctx, workerID, errorType, incidentID)

	if err := CheckDLQMarker(ctx, c.store, "Attempting to process postmortem generated event. Checking DLQ Marker.", "FSM_CONSUMER", "process_postmortem_generated", workerID, errorType, incidentID, correlationID); err != nil {
		return nil
	}

	existing, err := c.store.GetWorkerState(ctx, workerID, errorType)
	if err == nil && existing != nil && (existing.CurrentState == string(state.WorkerResolved) || existing.CurrentState == string(state.WorkerHealthy)) {
		slog.Debug("ignoring late postmortem event because FSM is already resolved or healthy", "component", "FSM_CONSUMER", "worker_id", workerID, "incident_id", incidentID, "state", existing.CurrentState)
		return nil
	}

	// Transition to POSTMORTEM_GENERATED
	if err := executeFSMTransition(ctx, c.store, c.publisher, c.producer, "FSM_CONSUMER", workerID, errorType, incidentID, correlationID, state.WorkerPostmortemGenerated); err != nil {
		return err
	}
	// Immediately transition to DELIVERY_IN_PROGRESS as delivery starts right after postmortem generation
	if err := executeFSMTransition(ctx, c.store, c.publisher, c.producer, "FSM_CONSUMER", workerID, errorType, incidentID, correlationID, state.WorkerDeliveryInProgress); err != nil {
		return err
	}

	// Check for a deferred DELIVERED event that arrived out-of-order
	deferredPayload, err := c.store.GetDeferredEvent(ctx, workerID, errorType, TopicDeliveryStatus)
	if err == nil && len(deferredPayload) > 0 {
		// TODO: see if single contract/schema is better
		var p deliveryResultPayload
		var defIncidentID string
		if err := json.Unmarshal(deferredPayload, &p); err == nil && p.IncidentID != "" {
			defIncidentID = p.IncidentID
		} else {
			var env state.EventEnvelope
			if err := json.Unmarshal(deferredPayload, &env); err == nil && env.IncidentID != "" {
				defIncidentID = env.IncidentID
			}
		}

		if defIncidentID != "" && !IsStale(ctx, c.store, workerID, errorType, defIncidentID) {
			slog.Info("found deferred DELIVERED event, fast-forwarding FSM", "component", "FSM_CONSUMER", "event", "DEFERRED_FOUND", "worker_id", workerID, "error_type", errorType)
			_ = c.store.DeleteDeferredEvent(ctx, workerID, errorType, TopicDeliveryStatus)
			return c.handleDeliveryStatus(ctx, TopicDeliveryStatus, deferredPayload)
		} else {
			slog.Warn("ignoring stale deferred DELIVERED event from old incident", "component", "FSM_CONSUMER", "event", "DEFERRED_STALE", "worker_id", workerID, "old_incident_id", defIncidentID, "current_incident_id", incidentID)
			_ = c.store.DeleteDeferredEvent(ctx, workerID, errorType, TopicDeliveryStatus)
		}
	}

	return nil
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

	if workerID == "" || errorType == "" || incidentID == "" {
		slog.Warn("could not find active worker state for delivery result", "component", "FSM_CONSUMER", "event", "DELIVERY_STATE_NOT_FOUND", "incident_id", incidentID, "topic", topic)
		return nil
	}

	if IsStale(ctx, c.store, workerID, errorType, incidentID) {
		slog.Debug("ignoring stale kafka event for old incident", "component", "FSM_CONSUMER", "worker_id", workerID)
		return nil
	}

	locked, err := c.store.AcquireFSMLock(ctx, workerID, errorType, incidentID, 5*time.Second)
	// TODO: instead of simply returning nil which could signal that no err occured keep the FSM stuck indefinitely, we need to return a specific err signal for this err so that the caller of the method can do a retry e.g. the main.fsmconsumer_loop routine will prevent a kafka commit which effectively retries processing on next loop
	// This has to be ensure throughout fsmconsumer, because loosing events that progresses the FSM is a fundamental flaw that reduces FSM efficacy, event though leaves FSM trustworthiness intact
	if err != nil || !locked {
		slog.Debug("could not acquire lock for delivery status, skipping", "worker_id", workerID, "error_type", errorType)
		return nil
	}
	defer c.store.ReleaseFSMLock(ctx, workerID, errorType, incidentID)

	if err := CheckDLQMarker(ctx, c.store, "Attempting to process delivery status event. Checking DLQ Marker.", "FSM_CONSUMER", "process_delivery_status", workerID, errorType, incidentID, payload.CorrelationID); err != nil {
		return nil
	}

	existing, err := c.store.GetWorkerState(ctx, workerID, errorType)
	if err == nil && existing != nil && (existing.CurrentState == string(state.WorkerResolved) || existing.CurrentState == string(state.WorkerHealthy)) {
		slog.Debug("ignoring late delivery status event because FSM is already resolved or healthy", "component", "FSM_CONSUMER", "worker_id", workerID, "incident_id", incidentID, "state", existing.CurrentState)
		return nil
	}

	isDLQ := topic == TopicDeliveryDLQ || payload.Status == "dlq"
	if isDLQ {
		if err := executeFSMTransition(ctx, c.store, c.publisher, c.producer, "FSM_CONSUMER", workerID, errorType, incidentID, payload.CorrelationID, state.WorkerDeliveryFailed); err != nil {
			return err
		}
	} else if payload.Status == "delivered" {
		fromState := state.WorkerHealthy
		if existing != nil && existing.CurrentState != "" {
			fromState = state.WorkerHealthState(existing.CurrentState)
		}

		if fromState == state.WorkerPostmortemRequested {
			// Early DELIVERED event race condition! Defer it instead of corrupting the FSM.
			ttlSec := 15*60 + 120 // 17 minutes default
			if val := os.Getenv("AEGIS_FSM_DEFERRAL_TTL_SECONDS"); val != "" {
				if i, err := strconv.Atoi(val); err == nil && i > 0 {
					ttlSec = i
				}
			} else if val := os.Getenv("AEGIS_FSM_WATCHDOG_STUCK_THRESHOLD_SECONDS"); val != "" {
				// Fallback for backwards compatibility
				if i, err := strconv.Atoi(val); err == nil && i > 0 {
					ttlSec = i + 120
				}
			}
			ttl := time.Duration(ttlSec) * time.Second
			if err := c.store.DeferEvent(ctx, workerID, errorType, TopicDeliveryStatus, value, ttl); err != nil {
				slog.Error("failed to defer early DELIVERED event", "component", "FSM_CONSUMER", "event", "DEFER_ERR", "error", err)
			} else {
				slog.Info("deferred early DELIVERED event to Redis", "component", "FSM_CONSUMER", "event", "DEFER_SUCCESS", "worker_id", workerID, "error_type", errorType)
			}
			return nil
		}

		if err := executeFSMTransition(ctx, c.store, c.publisher, c.producer, "FSM_CONSUMER", workerID, errorType, incidentID, payload.CorrelationID, state.WorkerDelivered); err != nil {
			return err
		}
	} else {
		slog.Debug("ignored non-terminal delivery status", "component", "FSM_CONSUMER", "event", "NON_TERMINAL_IGNORED", "status", payload.Status)
		return nil
	}

	// The automated FSM deliberately stops here.
	// Transition to RESOLVED must be explicitly requested via aegis-cli
	return nil
}
