package incident

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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
	/*
		Acquire short-lived mutex lock
		- for consistent hash divergence mutual exclusion
		- to prevent race condition by ensuring multiple executeFSMTransition() calls by a single process/goroutine in the cluster occur serially with mutual exclusion
		blocking other processes/goroutines from being able to transition or corrupt the FSM
	*/
	AcquireFSMLock(ctx context.Context, workerID string, errorType string, incidentID string, ttl time.Duration) (bool, error)
	ReleaseFSMLock(ctx context.Context, workerID string, errorType string, incidentID string) error
	SetWorkerState(ctx context.Context, st membership.WorkerState, topic string, partition int, offset int64) error
	// GetWorkerState retrieves the FSM state. etcd must be the authoritative source of truth
	// so that any CP replica can inherit the FSM if the original handling CP dies or the hash ring rebalances.
	// No CP should save or find worker/incident state locally in-memory.
	GetWorkerState(ctx context.Context, workerID string, errorType string) (*membership.WorkerState, error)
	DeleteWorkerState(ctx context.Context, workerID string, errorType string) error
	ListActiveWorkerStates(ctx context.Context) ([]membership.WorkerState, error)
	SetDLQMarker(ctx context.Context, workerID string, errorType string, markerData string) error
	GetDLQMarker(ctx context.Context, workerID string, errorType string) (string, error)
	DeleteDLQMarker(ctx context.Context, workerID string, errorType string) error
	ListDLQMarkers(ctx context.Context) ([]string, error)

	DeferEvent(ctx context.Context, workerID string, errorType string, eventType string, payload []byte, ttl time.Duration) error
	GetDeferredEvent(ctx context.Context, workerID string, errorType string, eventType string) ([]byte, error)
	DeleteDeferredEvent(ctx context.Context, workerID string, errorType string, eventType string) error
	ScanExpiringDeferredEvents(ctx context.Context, tolerance time.Duration) ([]membership.WorkerState, error)
}

type DiagnosticRequester interface {
	TriggerDiagnostics(ctx context.Context, req state.DiagnosticRequest) (state.DiagnosticBundle, error)
}

type Manager struct {
	store                    Store
	publisher                kafka.Publisher
	diagnostics              DiagnosticRequester
	producer                 string
	lockTTL                  time.Duration
	incidentIDTumblingWindow time.Duration
}

func NewManager(store Store, publisher kafka.Publisher, diagnostics DiagnosticRequester, producer string, incidentIDTumblingWindow time.Duration) *Manager {
	return &Manager{
		store:                    store,
		publisher:                publisher,
		diagnostics:              diagnostics,
		producer:                 producer,
		lockTTL:                  defaultIncidentLockTTL,
		incidentIDTumblingWindow: incidentIDTumblingWindow,
	}
}

func (m *Manager) HandleDetection(ctx context.Context, result state.DetectionResult) (state.Incident, bool, error) {
	// Pre-lock inspection
	var incidentID, correlationID string

	// Check if there is an existing state for this error type.
	// We read this from etcd (the authoritative state) rather than in-memory so we can inherit the FSM
	// from a previous CP that may have died or lost the agent due to hash ring rebalancing.
	existingState, err := m.store.GetWorkerState(ctx, result.WorkerID, string(result.FailureType))
	if err == nil && existingState != nil && existingState.IncidentID != "" && existingState.CurrentState != string(state.WorkerResolved) && existingState.CurrentState != string(state.WorkerHealthy) {
		// An incident is already in progress. Redundant telemetry detections are entirely expected
		// because the agent will continue to send failing metrics while the incident is being resolved.
		return state.Incident{}, false, nil
	} else {
		incidentID = DeterministicID(result.WorkerID, result.FailureType, result.ObservedAt, m.incidentIDTumblingWindow)
		if result.CorrelationID != "" {
			correlationID = result.CorrelationID
		} else {
			correlationID = fmt.Sprintf("corr-sys-%d", time.Now().UnixNano())
		}
	}

	locked, err := m.store.AcquireFSMLock(ctx, result.WorkerID, string(result.FailureType), incidentID, m.lockTTL)
	if err != nil || !locked {
		return state.Incident{}, false, err
	}
	defer func() {
		_ = m.store.ReleaseFSMLock(ctx, result.WorkerID, string(result.FailureType), incidentID)
	}()

	// Transition to WorkerSuspected is guaranteed to be valid because fromState is always WorkerHealthy

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

	if err := CheckDLQMarker(ctx, m.store, "Attempting to start a new FSM. Checking DLQ Marker.", "INCIDENT_MANAGER", "incident_initiation", inc.WorkerID, string(inc.FailureType), inc.IncidentID, inc.CorrelationID); err != nil {
		return state.Incident{}, false, nil
	}

	slog.Info("Incident detected and new FSM initiated", "component", "INCIDENT_MANAGER", "event", "FSM_INITIATED", "worker_id", inc.WorkerID, "incident_id", inc.IncidentID, "failure_type", inc.FailureType, "corr_id", inc.CorrelationID)

	if err := executeFSMTransition(ctx, m.store, m.publisher, m.producer, "INCIDENT_MANAGER", inc.WorkerID, string(inc.FailureType), inc.IncidentID, inc.CorrelationID, state.WorkerSuspected, "", 0, 0); err != nil {
		if errors.Is(err, ErrCorruptFSM) {
			return state.Incident{}, false, nil
		}
		return state.Incident{}, false, err
	}

	if err := m.publish(ctx, TopicIncidentDetected, "aegis.incident.detected", inc, fmt.Sprintf("%s:%s", result.WorkerID, result.FailureType), map[string]any{
		"reason":       reason,
		"failure_type": string(result.FailureType),
		"severity":     string(result.Severity),
	}); err != nil {
		return state.Incident{}, false, err
	}

	inc.State = state.WorkerDiagnosticsTriggered
	if err := executeFSMTransition(ctx, m.store, m.publisher, m.producer, "INCIDENT_MANAGER", inc.WorkerID, string(inc.FailureType), inc.IncidentID, inc.CorrelationID, state.WorkerDiagnosticsTriggered, "", 0, 0); err != nil {
		if errors.Is(err, ErrCorruptFSM) {
			return state.Incident{}, false, nil
		}
		return state.Incident{}, false, err
	}
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
	if err := executeFSMTransition(ctx, m.store, m.publisher, m.producer, "INCIDENT_MANAGER", inc.WorkerID, string(inc.FailureType), inc.IncidentID, inc.CorrelationID, state.WorkerDiagnosticsCollected, "", 0, 0); err != nil {
		if errors.Is(err, ErrCorruptFSM) {
			return state.Incident{}, false, nil
		}
		return state.Incident{}, false, err
	}
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
	if err := executeFSMTransition(ctx, m.store, m.publisher, m.producer, "INCIDENT_MANAGER", inc.WorkerID, string(inc.FailureType), inc.IncidentID, inc.CorrelationID, state.WorkerPostmortemRequested, "", 0, 0); err != nil {
		if errors.Is(err, ErrCorruptFSM) {
			return state.Incident{}, false, nil
		}
		return state.Incident{}, false, err
	}
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

func DeterministicID(workerID string, failureType state.FailureType, observedAt time.Time, incidentIDTumblingWindow time.Duration) string {
	if incidentIDTumblingWindow <= 0 {
		incidentIDTumblingWindow = time.Minute
	}
	bucket := observedAt.UTC().Truncate(incidentIDTumblingWindow).Format(time.RFC3339)
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%s|%s", workerID, failureType, bucket)))
	return "inc_" + hex.EncodeToString(sum[:])[:24]
}

func ValidTransition(from, to state.WorkerHealthState) bool {
	allowed := map[state.WorkerHealthState][]state.WorkerHealthState{
		state.WorkerHealthy:              {state.WorkerSuspected},
		state.WorkerResolved:             {state.WorkerSuspected},
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

// MarkFSMCorrupt is a DRY helper used by the FSM consumer and Watchdog to mark a worker's FSM as corrupt,
// freeze it in etcd, and emit an alert to the DLQ topic.
func MarkFSMCorrupt(ctx context.Context, store Store, publisher kafka.Publisher, producer string, inc state.Incident, toState state.WorkerHealthState, errMsg string, extraPayload map[string]any) error {
	if err := store.SetDLQMarker(ctx, inc.WorkerID, string(inc.FailureType), errMsg); err != nil {
		return fmt.Errorf("failed to set DLQ marker: %w", err)
	}

	// Purge any deferred events for this worker/error type to prevent them from leaking into future resolved states
	_ = store.DeleteDeferredEvent(ctx, inc.WorkerID, string(inc.FailureType), TopicDeliveryStatus)

	if publisher != nil {
		payload := map[string]any{
			"from_state": string(inc.State),
			"to_state":   string(toState),
			"error":      errMsg,
			"metric":     "aegis_fsm_illegal_transitions_total",
		}
		for k, v := range extraPayload {
			payload[k] = v
		}
		env := kafka.NewEnvelope("aegis.cp.fsm_illegal_transition", inc, producer, fmt.Sprintf("%s:%s", inc.WorkerID, inc.FailureType), payload, time.Now().UTC())
		if err := publisher.Publish(ctx, TopicCorruptFSMDLQ, env); err != nil {
			return fmt.Errorf("failed to publish to DLQ topic: %w", err)
		}
	}
	return nil
}
