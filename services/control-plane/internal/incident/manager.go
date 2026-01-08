package incident

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

const (
	TopicIncidentDetected      = "aegis.incident.detected"
	TopicDiagnosticsRequested  = "aegis.diagnostics.requested"
	TopicDiagnosticsCollected  = "aegis.diagnostics.collected"
	TopicPostmortemRequested   = "aegis.postmortem.requested"
	defaultIncidentLockTTL     = 10 * time.Minute
	defaultIncidentBucketWidth = time.Minute
)

type Locker interface {
	AcquireIncidentLock(ctx context.Context, workerID string, incidentID string, ttl time.Duration, now time.Time) (bool, error)
}

type DiagnosticRequester interface {
	TriggerDiagnostics(ctx context.Context, req state.DiagnosticRequest) (state.DiagnosticBundle, error)
}

type Manager struct {
	locker      Locker
	publisher   kafka.Publisher
	diagnostics DiagnosticRequester
	producer    string
	lockTTL     time.Duration
	bucketWidth time.Duration
}

func NewManager(locker Locker, publisher kafka.Publisher, diagnostics DiagnosticRequester, producer string) *Manager {
	return &Manager{
		locker:      locker,
		publisher:   publisher,
		diagnostics: diagnostics,
		producer:    producer,
		lockTTL:     defaultIncidentLockTTL,
		bucketWidth: defaultIncidentBucketWidth,
	}
}

func (m *Manager) HandleDetection(ctx context.Context, result state.DetectionResult) (state.Incident, bool, error) {
	incidentID := DeterministicID(result.WorkerID, result.FailureType, result.ObservedAt, m.bucketWidth)
	locked, err := m.locker.AcquireIncidentLock(ctx, result.WorkerID, incidentID, m.lockTTL, result.ObservedAt)
	if err != nil || !locked {
		return state.Incident{}, false, err
	}
	inc := state.Incident{
		IncidentID:    incidentID,
		WorkerID:      result.WorkerID,
		FailureType:   result.FailureType,
		State:         state.WorkerSuspected,
		Severity:      result.Severity,
		DetectedAt:    result.ObservedAt.UTC(),
		CorrelationID: result.CorrelationID,
	}
	if err := m.publish(ctx, TopicIncidentDetected, "aegis.incident.detected", inc, "", map[string]any{
		"reason":       result.Reason,
		"failure_type": string(result.FailureType),
		"severity":     string(result.Severity),
	}); err != nil {
		return state.Incident{}, false, err
	}

	inc.State = state.WorkerDiagnosticsTriggered
	if err := m.publish(ctx, TopicDiagnosticsRequested, "aegis.diagnostics.requested", inc, "", map[string]any{
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
	if err := m.publish(ctx, TopicDiagnosticsCollected, "aegis.diagnostics.collected", inc, "", map[string]any{
		"diagnostic_status": bundle.DiagnosticStatus,
		"diagnostics":       bundle.Payload,
	}); err != nil {
		return state.Incident{}, false, err
	}

	inc.State = state.WorkerPostmortemRequested
	if err := m.publish(ctx, TopicPostmortemRequested, "aegis.postmortem.requested", inc, "", map[string]any{
		"diagnostic_status": bundle.DiagnosticStatus,
		"postmortem_format": "default",
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

