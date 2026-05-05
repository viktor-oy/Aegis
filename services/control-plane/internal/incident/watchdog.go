package incident

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type RingProvider interface {
	Owner(workerID string) (hashring.Member, bool)
}

type Watchdog struct {
	store          Store
	publisher      kafka.Publisher
	producer       string
	cpID           string
	ring           RingProvider
	ringMu         sync.RWMutex
	interval       time.Duration
	stuckThreshold time.Duration
}

func NewWatchdog(store Store, publisher kafka.Publisher, producer string, cpID string) *Watchdog {
	intervalSec := 300
	if val := os.Getenv("AEGIS_FSM_WATCHDOG_INTERVAL_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			intervalSec = i
		}
	}
	return &Watchdog{
		store:          store,
		publisher:      publisher,
		producer:       producer,
		cpID:           cpID,
		interval:       time.Duration(intervalSec) * time.Second,
		stuckThreshold: 15 * time.Minute,
	}
}

func (w *Watchdog) UpdateRing(ring RingProvider) {
	w.ringMu.Lock()
	defer w.ringMu.Unlock()
	w.ring = ring
}

func (w *Watchdog) WithStuckThreshold(d time.Duration) *Watchdog {
	w.stuckThreshold = d
	return w
}

func (w *Watchdog) WithInterval(d time.Duration) *Watchdog {
	w.interval = d
	return w
}

// InspectOnce performs a scan of active worker states and DLQ markers, emitting logs and DLQ alerts for stuck or corrupt incidents without auto-resolving.
func (w *Watchdog) InspectOnce(ctx context.Context, now time.Time) (int, int, error) {
	stuckCount := 0
	corruptCount := 0

	states, err := w.store.ListActiveWorkerStates(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("fsm_watchdog: list active worker states: %w", err)
	}

	w.ringMu.RLock()
	ring := w.ring
	w.ringMu.RUnlock()

	for _, st := range states {
		if st.CurrentState == string(state.WorkerResolved) || st.CurrentState == string(state.WorkerHealthy) {
			continue
		}
		if ring != nil {
			owner, ok := ring.Owner(st.WorkerID)
			if ok && owner.ID != w.cpID {
				continue // not owned by this CP
			}
		}
		refTime := st.UpdatedAt
		if refTime.IsZero() {
			continue
		}

		elapsed := now.Sub(refTime)
		if elapsed >= w.stuckThreshold {
			stuckCount++
			slog.Warn("fsm_watchdog: stuck incident detected",
				"metric", "aegis_fsm_stuck_incident",
				"worker_id", st.WorkerID,
				"error_type", st.ErrorType,
				"incident_id", st.IncidentID,
				"current_state", st.CurrentState,
				"stuck_duration", elapsed.String(),
			)
			if w.publisher != nil {
				inc := state.Incident{
					IncidentID:    st.IncidentID,
					WorkerID:      st.WorkerID,
					FailureType:   state.FailureType(st.ErrorType),
					State:         state.WorkerHealthState(st.CurrentState),
					CorrelationID: st.CorrelationID,
				}
				env := kafka.NewEnvelope("aegis.cp.fsm_stuck_incident", inc, w.producer, fmt.Sprintf("%s:%s", st.WorkerID, st.ErrorType), map[string]any{
					"stuck_duration_seconds": int(elapsed.Seconds()),
					"metric":                 "aegis_fsm_stuck_incident",
				}, now)
				_ = w.publisher.Publish(ctx, TopicCorruptFSMDLQ, env)
			}
		}
	}

	markers, err := w.store.ListDLQMarkers(ctx)
	if err != nil {
		return stuckCount, 0, fmt.Errorf("fsm_watchdog: list dlq markers: %w", err)
	}

	for _, m := range markers {
		parts := strings.Split(m, ":")
		if len(parts) >= 6 {
			workerID := parts[4] // format: aegis:cp:dlq:corrupt:<workerID>:<errorType>
			if ring != nil {
				owner, ok := ring.Owner(workerID)
				if ok && owner.ID != w.cpID {
					continue // not owned by this CP
				}
			}
		}

		corruptCount++
		slog.Warn("fsm_watchdog: corrupt FSM marker detected in Redis",
			"metric", "aegis_fsm_corrupt_marker_detected",
			"marker", m,
		)
		if w.publisher != nil {
			inc := state.Incident{
				IncidentID:  "dlq-marker-alert",
				WorkerID:    m,
				FailureType: "CORRUPT_FSM_MARKER",
			}
			env := kafka.NewEnvelope("aegis.cp.fsm_corrupt_marker", inc, w.producer, m, map[string]any{
				"marker": m,
				"metric": "aegis_fsm_corrupt_marker_detected",
			}, now)
			_ = w.publisher.Publish(ctx, TopicCorruptFSMDLQ, env)
		}
	}

	return stuckCount, corruptCount, nil
}

// Start launches the background watchdog scanner loop until context is canceled.
func (w *Watchdog) Start(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _, _ = w.InspectOnce(ctx, time.Now().UTC())
		}
	}
}
