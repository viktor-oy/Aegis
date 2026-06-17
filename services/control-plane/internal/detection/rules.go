package detection

import (
	"fmt"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type Rules struct {
	TemperatureCelsius      float64
	VRAMPressureRatio       float64
	ECCBurstDelta           uint64
	LatencySpikeMS          float64
	SustainedSampleCount    int
	MissedHeartbeatSeverity state.Severity
}

func DefaultRules() Rules {
	return Rules{
		TemperatureCelsius:      85,
		VRAMPressureRatio:       0.92,
		ECCBurstDelta:           8,
		LatencySpikeMS:          2500,
		SustainedSampleCount:    3,
		MissedHeartbeatSeverity: state.SeverityCritical,
	}
}

type Window struct {
	rules   Rules
	samples []state.TelemetrySample
}

func NewWindow(rules Rules) *Window {
	if rules.SustainedSampleCount <= 0 {
		rules.SustainedSampleCount = 3
	}
	return &Window{rules: rules}
}

func (w *Window) Clear() {
	w.samples = nil
}

func (w *Window) IsFresh(sample state.TelemetrySample) bool {
	if len(w.samples) == 0 {
		return true
	}
	last := w.samples[len(w.samples)-1]
	// Using Agent's timestamp as a monotonic sequence number
	// If the new sample is older than or equal to the last seen sample, it's out of order or stale.
	return sample.Timestamp.After(last.Timestamp)
}

func (w *Window) Add(sample state.TelemetrySample) (state.DetectionResult, bool) {
	w.samples = append(w.samples, sample)
	limit := max(w.rules.SustainedSampleCount, 8)
	if len(w.samples) > limit {
		w.samples = append([]state.TelemetrySample(nil), w.samples[len(w.samples)-limit:]...)
	}
	return w.Evaluate()
}

func (w *Window) Evaluate() (state.DetectionResult, bool) {
	if len(w.samples) == 0 {
		return state.DetectionResult{}, false
	}
	latest := w.samples[len(w.samples)-1]
	if latest.SyntheticFailureFlag != "" && latest.SyntheticFailureFlag != "normal" {
		return detection(latest, state.FailureSynthetic, state.SeverityCritical, "agent reported synthetic failure flag"), true
	}
	if !latest.ModelServerHealthy {
		return detection(latest, state.FailureModelUnhealthy, state.SeverityCritical, "model server health check is unhealthy"), true
	}
	if w.sustained(func(s state.TelemetrySample) bool { return s.TemperatureCelsius >= w.rules.TemperatureCelsius }) {
		return detection(latest, state.FailureGPUOverheat, state.SeverityCritical, fmt.Sprintf("temperature sustained above %.1fC", w.rules.TemperatureCelsius)), true
	}
	if w.sustained(func(s state.TelemetrySample) bool { return s.VRAMRatio() >= w.rules.VRAMPressureRatio }) {
		return detection(latest, state.FailureVRAMPressure, state.SeverityWarning, fmt.Sprintf("vram pressure sustained above %.0f%%", w.rules.VRAMPressureRatio*100)), true
	}
	if len(w.samples) >= 2 {
		first := w.samples[0]
		if latest.ECCErrorCount >= first.ECCErrorCount+w.rules.ECCBurstDelta {
			return detection(latest, state.FailureECCBurst, state.SeverityCritical, "ecc error count burst detected"), true
		}
	}
	if w.sustained(func(s state.TelemetrySample) bool { return s.InferenceLatencyMS >= w.rules.LatencySpikeMS }) {
		return detection(latest, state.FailureLatencySpike, state.SeverityWarning, fmt.Sprintf("latency sustained above %.0fms", w.rules.LatencySpikeMS)), true
	}
	return state.DetectionResult{}, false
}

func MissedHeartbeat(workerID string, deadline time.Time, correlationID string, rules Rules) state.DetectionResult {
	return state.DetectionResult{
		WorkerID:      workerID,
		FailureType:   state.FailureMissedHeartbeat,
		Severity:      rules.MissedHeartbeatSeverity,
		Reason:        "heartbeat deadline expired",
		ObservedAt:    deadline,
		CorrelationID: correlationID,
	}
}

func (w *Window) sustained(match func(state.TelemetrySample) bool) bool {
	if len(w.samples) < w.rules.SustainedSampleCount {
		return false
	}
	start := len(w.samples) - w.rules.SustainedSampleCount
	for _, sample := range w.samples[start:] {
		if !match(sample) {
			return false
		}
	}
	return true
}

func detection(sample state.TelemetrySample, failure state.FailureType, severity state.Severity, reason string) state.DetectionResult {
	return state.DetectionResult{
		WorkerID:      sample.WorkerID,
		FailureType:   failure,
		Severity:      severity,
		Reason:        reason,
		ObservedAt:    sample.Timestamp,
		CorrelationID: sample.CorrelationID,
	}
}
