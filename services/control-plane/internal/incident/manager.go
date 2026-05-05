package incident

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

const (
	TopicIncidentDetected      = "aegis.incident.detected"
	TopicDiagnosticsRequested  = "aegis.diagnostics.requested"
	TopicDiagnosticsCollected  = "aegis.diagnostics.collected"
	TopicPostmortemRequested   = "aegis.postmortem.requested"
	TopicCorruptFSMDLQ         = "aegis.cp.corrupt-fsm.dlq"
	defaultIncidentLockTTL     = 10 * time.Minute
	defaultIncidentBucketWidth = time.Minute
)

type Store interface {
	AcquireIncidentLock(ctx context.Context, workerID string, incidentID string, ttl time.Duration, now time.Time) (bool, error)
	ReleaseIncidentLock(ctx context.Context, workerID string, incidentID string) error
	SetWorkerState(ctx context.Context, st membership.WorkerState) error
	GetWorkerState(ctx context.Context, workerID string, errorType string) (*membership.WorkerState, error)
	DeleteWorkerState(ctx context.Context, workerID string, errorType string) error
	ListActiveWorkerStates(ctx context.Context) ([]membership.WorkerState, error)
	SetDLQMarker(ctx context.Context, workerID string, errorType string, markerData string) error
	GetDLQMarker(ctx context.Context, workerID string, errorType string) (string, error)
	DeleteDLQMarker(ctx context.Context, workerID string, errorType string) error
	ListDLQMarkers(ctx context.Context) ([]string, error)
}

type DiagnosticRequester interface {
	TriggerDiagnostics(ctx context.Context, req state.DiagnosticRequest) (state.DiagnosticBundle, error)
}

type Manager struct {
	store       Store
	publisher   kafka.Publisher
	diagnostics DiagnosticRequester
	producer    string
	lockTTL     time.Duration
	bucketWidth time.Duration
}

func NewManager(store Store, publisher kafka.Publisher, diagnostics DiagnosticRequester, producer string) *Manager {
	return &Manager{
		store:       store,
		publisher:   publisher,
		diagnostics: diagnostics,
		producer:    producer,
		lockTTL:     defaultIncidentLockTTL,
		bucketWidth: defaultIncidentBucketWidth,
	}
}

func (m *Manager) HandleDetection(ctx context.Context, result state.DetectionResult) (state.Incident, bool, error) {
	// Pre-lock inspection
	var incidentID, correlationID string
	fromState := state.WorkerHealthy

	// Check if there is an existing state for this error type
	existingState, err := m.store.GetWorkerState(ctx, result.WorkerID, string(result.FailureType))
	if err == nil && existingState != nil && existingState.IncidentID != "" && existingState.CurrentState != string(state.WorkerResolved) {
		// Inherit FSM from previous CP that may be dead or lost the agent being handled due to hashring rebalance
		incidentID = existingState.IncidentID
		correlationID = existingState.CorrelationID
		fromState = state.WorkerHealthState(existingState.CurrentState)
	} else {
		incidentID = DeterministicID(result.WorkerID, result.FailureType, result.ObservedAt, m.bucketWidth)
		correlationID = result.CorrelationID
	}

	// Acquire short-lived mutex lock solely for consistent hash divergence mutual exclusion
	locked, err := m.store.AcquireIncidentLock(ctx, result.WorkerID, incidentID, m.lockTTL, result.ObservedAt)
	if err != nil || !locked {
		return state.Incident{}, false, err
	}
	defer func() {
		_ = m.store.ReleaseIncidentLock(ctx, result.WorkerID, incidentID)
	}()

	// Enforce FSM validator matrix before transitioning to WorkerSuspected
	if !ValidTransition(fromState, state.WorkerSuspected) {
		slog.Error("illegal FSM transition rejected",
			"metric", "aegis_fsm_illegal_transitions_total",
			"worker_id", result.WorkerID,
			"error_type", string(result.FailureType),
			"incident_id", incidentID,
			"from_state", string(fromState),
			"to_state", string(state.WorkerSuspected),
			"correlation_id", correlationID,
		)
		errMsg := fmt.Sprintf("illegal FSM transition from %s to %s", fromState, state.WorkerSuspected)
		_ = m.store.SetDLQMarker(ctx, result.WorkerID, string(result.FailureType), errMsg)
		inc := state.Incident{
			IncidentID:    incidentID,
			WorkerID:      result.WorkerID,
			FailureType:   result.FailureType,
			State:         fromState,
			Severity:      result.Severity,
			DetectedAt:    result.ObservedAt.UTC(),
			CorrelationID: correlationID,
		}
		_ = m.publish(ctx, TopicCorruptFSMDLQ, "aegis.cp.fsm_illegal_transition", inc, fmt.Sprintf("%s:%s", result.WorkerID, result.FailureType), map[string]any{
			"from_state": string(fromState),
			"to_state":   string(state.WorkerSuspected),
			"error":      errMsg,
			"metric":     "aegis_fsm_illegal_transitions_total",
		})
		return state.Incident{}, false, fmt.Errorf("illegal FSM transition from %s to %s for worker %s", fromState, state.WorkerSuspected, result.WorkerID)
	}

	reason := result.Reason
	if result.FailureType == state.FailureMissedHeartbeat {
		reason = "Node failure suspected due to missed heartbeat min-heap expiry"
	}

	inc := state.Incident{
		IncidentID:    incidentID,
		WorkerID:      result.WorkerID,
		FailureType:   result.FailureType,
		State:         state.WorkerSuspected,
		Severity:      result.Severity,
		DetectedAt:    result.ObservedAt.UTC(),
		CorrelationID: correlationID,
	}
	_ = m.store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:      result.WorkerID,
		ErrorType:     string(result.FailureType),
		IncidentID:    inc.IncidentID,
		CurrentState:  string(inc.State),
		CorrelationID: inc.CorrelationID,
		UpdatedAt:     time.Now().UTC(),
	})

	if err := m.publish(ctx, TopicIncidentDetected, "aegis.incident.detected", inc, fmt.Sprintf("%s:%s", result.WorkerID, result.FailureType), map[string]any{
		"reason":       reason,
		"failure_type": string(result.FailureType),
		"severity":     string(result.Severity),
	}); err != nil {
		return state.Incident{}, false, err
	}

	inc.State = state.WorkerDiagnosticsTriggered
	_ = m.store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:      result.WorkerID,
		ErrorType:     string(result.FailureType),
		IncidentID:    inc.IncidentID,
		CurrentState:  string(inc.State),
		CorrelationID: inc.CorrelationID,
		UpdatedAt:     time.Now().UTC(),
	})
	if err := m.publish(ctx, TopicDiagnosticsRequested, "aegis.diagnostics.requested", inc, fmt.Sprintf("%s:%s", result.WorkerID, result.FailureType), map[string]any{
		"diagnostic_status": "requested",
	}); err != nil {
		return state.Incident{}, false, err
	}

	bundle, diagErr := m.diagnostics.TriggerDiagnostics(ctx, state.DiagnosticRequest{
		WorkerID:      inc.WorkerID,
		IncidentID:    inc.IncidentID,
		FailureType:   inc.FailureType,
		RequestedAt:   result.ObservedAt,
		CorrelationID: inc.CorrelationID,
	})
	if diagErr != nil {
		bundle = state.DiagnosticBundle{
			WorkerID:         inc.WorkerID,
			IncidentID:       inc.IncidentID,
			CollectedAt:      result.ObservedAt,
			DiagnosticStatus: "unavailable",
			Payload: map[string]any{
				"error": diagErr.Error(),
				"note":  "agent unreachable; using last known telemetry window",
			},
			CorrelationID: inc.CorrelationID,
		}
	}
	inc.State = state.WorkerDiagnosticsCollected
	_ = m.store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:      result.WorkerID,
		ErrorType:     string(result.FailureType),
		IncidentID:    inc.IncidentID,
		CurrentState:  string(inc.State),
		CorrelationID: inc.CorrelationID,
		UpdatedAt:     time.Now().UTC(),
	})
	if err := m.publish(ctx, TopicDiagnosticsCollected, "aegis.diagnostics.collected", inc, fmt.Sprintf("%s:%s", result.WorkerID, result.FailureType), map[string]any{
		"diagnostic_status": bundle.DiagnosticStatus,
		"diagnostics":       bundle.Payload,
		"reason":            reason,
		"failure_type":      string(result.FailureType),
		"severity":          string(result.Severity),
	}); err != nil {
		return state.Incident{}, false, err
	}

	inc.State = state.WorkerPostmortemRequested
	_ = m.store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:      result.WorkerID,
		ErrorType:     string(result.FailureType),
		IncidentID:    inc.IncidentID,
		CurrentState:  string(inc.State),
		CorrelationID: inc.CorrelationID,
		UpdatedAt:     time.Now().UTC(),
	})
	if err := m.publish(ctx, TopicPostmortemRequested, "aegis.postmortem.requested", inc, fmt.Sprintf("%s:%s", result.WorkerID, result.FailureType), map[string]any{
		"diagnostic_status": bundle.DiagnosticStatus,
		"postmortem_format": "default",
		"diagnostics":       bundle.Payload,
		"reason":            reason,
		"failure_type":      string(result.FailureType),
		"severity":          string(result.Severity),
	}); err != nil {
		return state.Incident{}, false, err
	}
	return inc, true, nil
}

func DeterministicID(workerID string, failureType state.FailureType, observedAt time.Time, bucketWidth time.Duration) string {
	if bucketWidth <= 0 {
		bucketWidth = time.Minute
	}
	bucket := observedAt.UTC().Truncate(bucketWidth).Format(time.RFC3339)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s", workerID, failureType, bucket)))
	return "inc_" + hex.EncodeToString(sum[:])[:24]
}

func ValidTransition(from, to state.WorkerHealthState) bool {
	allowed := map[state.WorkerHealthState][]state.WorkerHealthState{
		state.WorkerHealthy:              {state.WorkerSuspected},
		state.WorkerSuspected:            {state.WorkerDiagnosticsTriggered, state.WorkerResolved},
		state.WorkerDiagnosticsTriggered: {state.WorkerDiagnosticsCollected},
		state.WorkerDiagnosticsCollected: {state.WorkerPostmortemRequested},
		state.WorkerPostmortemRequested:  {state.WorkerPostmortemGenerated},
		state.WorkerPostmortemGenerated:  {state.WorkerDeliveryInProgress},
		state.WorkerDeliveryInProgress:   {state.WorkerDelivered, state.WorkerDeliveryFailed},
		state.WorkerDeliveryFailed:       {state.WorkerResolved},
		state.WorkerDelivered:            {state.WorkerResolved},
	}
	for _, candidate := range allowed[from] {
		if candidate == to {
			return true
		}
	}
	return false
}

func (m *Manager) publish(ctx context.Context, topic string, eventType string, inc state.Incident, causationID string, payload map[string]any) error {
	env := kafka.NewEnvelope(eventType, inc, m.producer, causationID, payload, time.Now())
	return m.publisher.Publish(ctx, topic, env)
}
