package incident

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

var ErrCorruptFSM = errors.New("FSM is corrupt (DLQ marker exists)")

func IsStale(ctx context.Context, store Store, workerID, errorType, incidentID string) bool {
	existing, err := store.GetWorkerState(ctx, workerID, errorType)
	return err == nil && existing != nil && existing.IncidentID != incidentID
}

// CheckDLQMarker checks if a DLQ marker exists for the given worker and error type.
// It logs an initial debug message to indicate the check is starting.
func CheckDLQMarker(ctx context.Context, store Store, msg string, component string, actionContext string, workerID string, errorType string, incidentID string, correlationID string) error {
	slog.Debug(msg,
		"component", component,
		"event", "FSM_CORRUPT_CHECK",
		"action", actionContext,
		"worker_id", workerID,
		"error_type", errorType,
		"incident_id", incidentID,
		"corr_id", correlationID,
	)

	marker, err := store.GetDLQMarker(ctx, workerID, errorType)
	if err == nil && marker != "" {
		slog.Debug("FSM is corrupt (DLQ marker exists)",
			"component", component,
			"event", "FSM_CORRUPT_BAILOUT",
			"action", actionContext,
			"worker_id", workerID,
			"error_type", errorType,
			"incident_id", incidentID,
			"corr_id", correlationID,
		)
		return ErrCorruptFSM
	}
	return nil
}

// executeFSMTransition performs the core state machine validation, updates the authoritative state in etcd,
// and conditionally publishes an event if a publisher is provided.
func executeFSMTransition(
	ctx context.Context,
	store Store,
	publisher kafka.Publisher,
	producer string,
	component string,
	workerID string,
	errorType string,
	incidentID string,
	correlationID string,
	toState state.WorkerHealthState,
	topic string,
	partition int,
	offset int64,
) error {
	existing, err := store.GetWorkerState(ctx, workerID, errorType)
	fromState := state.WorkerHealthy
	if err == nil && existing != nil && existing.CurrentState != "" {
		fromState = state.WorkerHealthState(existing.CurrentState)
	}

	// Allow idempotent retries for in-progress states.
	// We strictly enforce integrity for Healthy and Resolved states.
	if fromState == toState && fromState != state.WorkerHealthy && fromState != state.WorkerResolved {
		return nil
	}

	if !ValidTransition(fromState, toState) {
		slog.Error("illegal FSM transition rejected hence corrupted",
			"component", component,
			"event", "TRANSITION_REJECTED",
			"metric", "aegis_fsm_illegal_transitions_total",
			"worker_id", workerID,
			"error_type", errorType,
			"incident_id", incidentID,
			"from_state", string(fromState),
			"to_state", string(toState),
			"correlation_id", correlationID,
		)
		inc := state.Incident{
			IncidentID:    incidentID,
			WorkerID:      workerID,
			FailureType:   state.FailureType(errorType),
			State:         fromState,
			CorrelationID: correlationID,
		}
		errMsg := fmt.Sprintf("illegal FSM transition from %s to %s", fromState, toState)
		if err := MarkFSMCorrupt(ctx, store, publisher, producer, inc, toState, errMsg, nil); err != nil {
			slog.Error("failed to mark FSM as corrupt", "component", component, "event", "MARK_CORRUPT_ERR", "worker_id", workerID, "error_type", errorType, "error", err)
		} else {
			slog.Info("successfully marked FSM as corrupt", "component", component, "event", "MARK_CORRUPT_SUCCESS", "worker_id", workerID, "error_type", errorType)
		}
		return fmt.Errorf("illegal FSM transition from %s to %s for worker %s", fromState, toState, workerID)
	}

	err = store.SetWorkerState(ctx, membership.WorkerState{
		WorkerID:      workerID,
		ErrorType:     errorType,
		IncidentID:    incidentID,
		CurrentState:  string(toState),
		CorrelationID: correlationID,
		UpdatedAt:     time.Now().UTC(),
	}, topic, partition, offset)

	if err == nil {
		slog.Info("successful FSM transition",
			"component", component,
			"event", "FSM_TRANSITION",
			"worker_id", workerID,
			"error_type", errorType,
			"incident_id", incidentID,
			"from_state", string(fromState),
			"to_state", string(toState),
		)
	}

	return err
}
