package membership

import (
	"context"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
)

type Store interface {
	Register(ctx context.Context, member hashring.Member, ttl time.Duration, now time.Time) error
	Refresh(ctx context.Context, member hashring.Member, ttl time.Duration, now time.Time) error
	ActiveMembers(ctx context.Context, now time.Time) ([]hashring.Member, error)
	AcquireIncidentLock(ctx context.Context, workerID string, incidentID string, ttl time.Duration, now time.Time) (bool, error)
}

