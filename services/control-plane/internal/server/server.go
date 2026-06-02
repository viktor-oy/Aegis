package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/detection"
	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/services/control-plane/internal/heartbeat"
	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

var ErrResourceExhausted = errors.New("control-plane ingest queue exhausted")

type RingProvider interface {
	Owner(workerID string) (hashring.Member, bool)
}

type Directive struct {
	Type          string
	OwnerHint     string
	Message       string
	CorrelationID string
}

type ControlPlane struct {
	id      string
	address string
	ring    RingProvider
	manager *incident.Manager
	tracker *heartbeat.Tracker
	timeout time.Duration
	queue   chan state.TelemetrySample
	windows map[string]*detection.Window
	rules   detection.Rules
	mu      sync.Mutex
}

func New(id string, address string, ring RingProvider, manager *incident.Manager, queueSize int, heartbeatTimeout time.Duration) *ControlPlane {
	if queueSize <= 0 {
		queueSize = 64
	}
	rules := detection.DefaultRules()
	return &ControlPlane{
		id:      id,
		address: address,
		ring:    ring,
		manager: manager,
		tracker: heartbeat.NewTracker(heartbeatTimeout),
		timeout: 25 * time.Millisecond,
		queue:   make(chan state.TelemetrySample, queueSize),
		windows: map[string]*detection.Window{},
		rules:   rules,
	}
}

func (cp *ControlPlane) UpdateRing(ring RingProvider) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	cp.ring = ring
}

func (cp *ControlPlane) currentRing() RingProvider {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.ring
}

func (cp *ControlPlane) Ingest(ctx context.Context, sample state.TelemetrySample) (Directive, error) {
	if err := ctx.Err(); err != nil {
		slog.Warn("ingest context canceled", "component", "GRPC_SERVER", "event", "INGEST_CANCEL", "worker_id", sample.WorkerID, "error", err)
		return Directive{}, err
	}

	ring := cp.currentRing()
	owner, ok := ring.Owner(sample.WorkerID)
	if ok && owner.ID != cp.id {
		slog.Info("redirecting worker", "component", "GRPC_SERVER", "event", "WORKER_REDIRECT", "worker_id", sample.WorkerID, "from", cp.id, "to", owner.ID, "hint", owner.Address)
		return Directive{
			Type:          "redirect",
			OwnerHint:     owner.Address,
			Message:       "worker is owned by another control-plane replica",
			CorrelationID: sample.CorrelationID,
		}, nil
	}

	select {
	case cp.queue <- sample:
		slog.Debug("telemetry accepted and queued", "component", "GRPC_SERVER", "event", "TELEMETRY_QUEUED", "worker_id", sample.WorkerID, "corr_id", sample.CorrelationID, "error_type", sample.SyntheticFailureFlag)
		return Directive{Type: "accepted", CorrelationID: sample.CorrelationID}, nil
	default:
		slog.Warn("ingest queue exhausted", "component", "GRPC_SERVER", "event", "QUEUE_EXHAUSTED", "worker_id", sample.WorkerID)
		return Directive{}, ErrResourceExhausted
	}
}

func (cp *ControlPlane) ProcessOne(ctx context.Context) (bool, error) {
	select {
	case sample := <-cp.queue:
		cp.tracker.Observe(sample.WorkerID, sample.Timestamp)
		result, detected := cp.addAndEvaluate(sample)
		if detected {
			slog.Warn("failure detected for worker", "component", "GRPC_SERVER", "event", "AGENT_FAILURE_DETECTED", "worker_id", sample.WorkerID, "reason", result.Reason)
			_, _, err := cp.manager.HandleDetection(ctx, result)
			if err != nil {
				return true, fmt.Errorf("failed to handle detection for worker %s: %w", sample.WorkerID, err)
			}
			return true, nil
		}
		return true, nil
	case <-ctx.Done():
		slog.Debug("processing loop context canceled", "component", "GRPC_SERVER", "event", "PROCESS_CANCEL")
		return false, ctx.Err()
	default:
		return false, nil
	}
}

func (cp *ControlPlane) ExpireHeartbeats(ctx context.Context, now time.Time) error {
	for _, expired := range cp.tracker.Expired(now) {
		owner, ok := cp.ring.Owner(expired.WorkerID)
		if ok && owner.ID != cp.id {
			continue
		}
		result := detection.MissedHeartbeat(expired.WorkerID, expired.DeadlineAt, "", cp.rules)
		if _, _, err := cp.manager.HandleDetection(ctx, result); err != nil {
			return err
		}
	}
	return nil
}

func (cp *ControlPlane) QueueDepth() int {
	return len(cp.queue)
}

// NextHeartbeatDeadline returns the earliest pending heartbeat deadline from
// the min-heap. The caller uses this to set a precise timer instead of polling
// on a fixed interval.
func (cp *ControlPlane) NextHeartbeatDeadline() (heartbeat.HeartbeatDeadline, bool) {
	return cp.tracker.NextDeadline()
}

func (cp *ControlPlane) addAndEvaluate(sample state.TelemetrySample) (state.DetectionResult, bool) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	window := cp.windows[sample.WorkerID]
	if window == nil {
		window = detection.NewWindow(cp.rules)
		cp.windows[sample.WorkerID] = window
	}
	result, detected := window.Add(sample)
	if detected {
		window.Clear()
	}
	return result, detected
}
