package membership

import (
	"context"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
)

type WorkerState struct {
	WorkerID      string    `json:"worker_id"`
	ErrorType     string    `json:"error_type"`
	IncidentID    string    `json:"incident_id"`
	CurrentState  string    `json:"current_state"`
	CorrelationID string    `json:"correlation_id"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Store interface {
	Register(ctx context.Context, member hashring.Member, ttl time.Duration, now time.Time) error
	Refresh(ctx context.Context, member hashring.Member, ttl time.Duration, now time.Time) error
	ActiveMembers(ctx context.Context, now time.Time) ([]hashring.Member, error)
	AcquireFSMLock(ctx context.Context, workerID string, errorType string, incidentID string, ttl time.Duration) (bool, error)
	ReleaseFSMLock(ctx context.Context, workerID string, errorType string, incidentID string) error

	SetWorkerState(ctx context.Context, state WorkerState, topic string, partition int, offset int64) error
	GetWorkerState(ctx context.Context, workerID string, errorType string) (*WorkerState, error)
	DeleteWorkerState(ctx context.Context, workerID string, errorType string) error
	ListActiveWorkerStates(ctx context.Context) ([]WorkerState, error)

	SetDLQMarker(ctx context.Context, workerID string, errorType string, markerData string) error
	GetDLQMarker(ctx context.Context, workerID string, errorType string) (string, error)
	DeleteDLQMarker(ctx context.Context, workerID string, errorType string) error
	ListDLQMarkers(ctx context.Context) ([]string, error)

	DeferEvent(ctx context.Context, workerID string, errorType string, eventType string, payload []byte, ttl time.Duration) error
	GetDeferredEvent(ctx context.Context, workerID string, errorType string, eventType string) ([]byte, error)
	DeleteDeferredEvent(ctx context.Context, workerID string, errorType string, eventType string) error
	ScanExpiringDeferredEvents(ctx context.Context, tolerance time.Duration) ([]WorkerState, error)
}
