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
	
	thresholdSec := 15 * 60 // 15 minutes default
	if val := os.Getenv("AEGIS_FSM_WATCHDOG_STUCK_THRESHOLD_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			thresholdSec = i
		}
	}

	return &Watchdog{
		store:          store,
		publisher:      publisher,
		producer:       producer,
		cpID:           cpID,
		interval:       time.Duration(intervalSec) * time.Second,
		stuckThreshold: time.Duration(thresholdSec) * time.Second,
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
			slog.Warn("stuck incident detected", "component", "WATCHDOG", "event", "STUCK_INCIDENT",
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
		slog.Warn("corrupt FSM marker detected in Redis", "component", "WATCHDOG", "event", "CORRUPT_FSM",
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

	toleranceSec := 120
	if val := os.Getenv("AEGIS_FSM_DEFERRAL_TOLERANCE_SECONDS"); val != "" {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			toleranceSec = i
		}
	}
	expiringDeferrals, err := w.store.ScanExpiringDeferredEvents(ctx, time.Duration(toleranceSec)*time.Second)
	if err != nil {
		return stuckCount, corruptCount, fmt.Errorf("fsm_watchdog: scan expiring deferrals: %w", err)
	}

	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		slog.Debug("scanned expiring deferrals", "component", "WATCHDOG", "event", "DEFERRALS_SCANNED", "count", len(expiringDeferrals))
	} else if len(expiringDeferrals) > 0 {
		slog.Info("scanned expiring deferrals", "component", "WATCHDOG", "event", "DEFERRALS_SCANNED", "count", len(expiringDeferrals))
	}

	for _, d := range expiringDeferrals {
		if ring != nil {
			owner, ok := ring.Owner(d.WorkerID)
			if ok && owner.ID != w.cpID {
				continue // not owned by this CP
			}
		}

		slog.Warn("deferred FSM event is expiring without prerequisites", "component", "WATCHDOG", "event", "DEFERRAL_EXPIRING",
			"worker_id", d.WorkerID,
			"error_type", d.ErrorType,
		)

		errMsg := fmt.Sprintf("FSM Corruption: deferred %s event expired without receiving prerequisite %s", state.WorkerDelivered, state.WorkerPostmortemGenerated)
		
		var inc state.Incident
		inc.WorkerID = d.WorkerID
		inc.FailureType = state.FailureType(d.ErrorType)
		inc.State = state.WorkerHealthy
		for _, st := range states {
			if st.WorkerID == d.WorkerID && st.ErrorType == d.ErrorType {
				inc.IncidentID = st.IncidentID
				inc.CorrelationID = st.CorrelationID
				inc.State = state.WorkerHealthState(st.CurrentState)
				break
			}
		}

		if inc.IncidentID != "" && IsStale(ctx, w.store, d.WorkerID, d.ErrorType, inc.IncidentID) {
			slog.Debug("ignoring stale expiring deferral", "component", "WATCHDOG", "worker_id", d.WorkerID, "incident_id", inc.IncidentID)
			_ = w.store.DeleteDeferredEvent(ctx, d.WorkerID, d.ErrorType, TopicDeliveryStatus)
			continue
		}

		if err := MarkFSMCorrupt(ctx, w.store, w.publisher, w.producer, inc, state.WorkerDelivered, errMsg, map[string]any{"deferred": true}); err != nil {
			slog.Error("failed to mark FSM as corrupt", "component", "WATCHDOG", "event", "MARK_CORRUPT_ERR", "worker_id", d.WorkerID, "error_type", d.ErrorType, "error", err)
		} else {
			slog.Info("successfully marked FSM as corrupt", "component", "WATCHDOG", "event", "MARK_CORRUPT_SUCCESS", "worker_id", d.WorkerID, "error_type", d.ErrorType)
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
