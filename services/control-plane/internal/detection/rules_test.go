package detection

import (
	"testing"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/state"
)

func TestSustainedTemperatureDetection(t *testing.T) {
	window := NewWindow(DefaultRules())
	now := time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 2; i++ {
		if _, detected := window.Add(sample(now.Add(time.Duration(i)*time.Second), 88, true)); detected {
			t.Fatal("temperature must be sustained before detection")
		}
	}
	result, detected := window.Add(sample(now.Add(3*time.Second), 89, true))
	if !detected {
		t.Fatal("expected sustained overheat detection")
	}
	if result.FailureType != state.FailureGPUOverheat {
		t.Fatalf("unexpected failure type %s", result.FailureType)
	}
}

func TestModelServerUnhealthyIsImmediate(t *testing.T) {
	window := NewWindow(DefaultRules())
	result, detected := window.Add(sample(time.Now(), 65, false))
	if !detected || result.FailureType != state.FailureModelUnhealthy {
		t.Fatalf("expected model server unhealthy detection, got %#v detected=%v", result, detected)
	}
}

func TestSustainedVRAMPressureDetection(t *testing.T) {
	window := NewWindow(DefaultRules())
	now := time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		s := state.TelemetrySample{
			WorkerID:           "worker-vram",
			Timestamp:          now.Add(time.Duration(i) * time.Second),
			VRAMUsedBytes:      95,
			VRAMTotalBytes:     100, // 95% > 92% threshold
			TemperatureCelsius: 60,
			ModelServerHealthy: true,
		}
		result, detected := window.Add(s)
		if i < 2 && detected {
			t.Fatal("VRAM pressure must be sustained before detection")
		}
		if i == 2 {
			if !detected {
				t.Fatal("expected sustained VRAM pressure detection")
			}
			if result.FailureType != state.FailureVRAMPressure {
				t.Fatalf("unexpected failure type %s", result.FailureType)
			}
		}
	}
}

func TestECCBurstDetection(t *testing.T) {
	window := NewWindow(DefaultRules())
	now := time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC)

	// First sample with baseline ECC count.
	s1 := state.TelemetrySample{
		WorkerID:           "worker-ecc",
		Timestamp:          now,
		ECCErrorCount:      10,
		TemperatureCelsius: 60,
		ModelServerHealthy: true,
	}
	if _, detected := window.Add(s1); detected {
		t.Fatal("single sample should not trigger ECC burst")
	}

	// Second sample with ECC count delta >= 8.
	s2 := s1
	s2.Timestamp = now.Add(time.Second)
	s2.ECCErrorCount = 19 // delta = 9, threshold = 8
	result, detected := window.Add(s2)
	if !detected {
		t.Fatal("expected ECC burst detection")
	}
	if result.FailureType != state.FailureECCBurst {
		t.Fatalf("unexpected failure type %s", result.FailureType)
	}
}

func TestSustainedLatencySpikeDetection(t *testing.T) {
	window := NewWindow(DefaultRules())
	now := time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		s := state.TelemetrySample{
			WorkerID:           "worker-latency",
			Timestamp:          now.Add(time.Duration(i) * time.Second),
			InferenceLatencyMS: 3000, // > 2500ms threshold
			TemperatureCelsius: 60,
			ModelServerHealthy: true,
		}
		result, detected := window.Add(s)
		if i < 2 && detected {
			t.Fatal("latency spike must be sustained before detection")
		}
		if i == 2 {
			if !detected {
				t.Fatal("expected sustained latency spike detection")
			}
			if result.FailureType != state.FailureLatencySpike {
				t.Fatalf("unexpected failure type %s", result.FailureType)
			}
		}
	}
}

func TestSyntheticFailureFlagDetection(t *testing.T) {
	window := NewWindow(DefaultRules())
	s := state.TelemetrySample{
		WorkerID:             "worker-synth",
		Timestamp:            time.Now(),
		TemperatureCelsius:   60,
		ModelServerHealthy:   true,
		SyntheticFailureFlag: "overheating",
	}
	result, detected := window.Add(s)
	if !detected {
		t.Fatal("expected synthetic failure detection")
	}
	if result.FailureType != state.FailureSynthetic {
		t.Fatalf("unexpected failure type %s", result.FailureType)
	}
}

func TestSyntheticNormalFlagNoDetection(t *testing.T) {
	window := NewWindow(DefaultRules())
	s := state.TelemetrySample{
		WorkerID:             "worker-normal",
		Timestamp:            time.Now(),
		TemperatureCelsius:   60,
		ModelServerHealthy:   true,
		SyntheticFailureFlag: "normal",
	}
	if _, detected := window.Add(s); detected {
		t.Fatal("synthetic failure flag 'normal' should not trigger detection")
	}
}

func TestTransientConditionNoDetection(t *testing.T) {
	window := NewWindow(DefaultRules())
	now := time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC)

	// Two high-temp samples followed by a normal one — not sustained.
	window.Add(sample(now, 90, true))
	window.Add(sample(now.Add(time.Second), 90, true))
	result, detected := window.Add(sample(now.Add(2*time.Second), 60, true)) // back to normal
	if detected {
		t.Fatalf("transient condition should not trigger detection, got %#v", result)
	}
}

func TestWindowOverflowKeepsRecentSamples(t *testing.T) {
	rules := DefaultRules()
	window := NewWindow(rules)
	now := time.Date(2026, 2, 2, 12, 0, 0, 0, time.UTC)

	// Add many healthy samples to fill the window.
	for i := 0; i < 20; i++ {
		window.Add(sample(now.Add(time.Duration(i)*time.Second), 60, true))
	}

	// Now add sustained high temp — should still detect.
	for i := 0; i < 3; i++ {
		_, detected := window.Add(sample(now.Add(time.Duration(20+i)*time.Second), 90, true))
		if i == 2 && !detected {
			t.Fatal("detection should work after window overflow")
		}
	}
}

func TestMissedHeartbeatResult(t *testing.T) {
	rules := DefaultRules()
	deadline := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	result := MissedHeartbeat("worker-lost", deadline, "corr-hb", rules)
	if result.FailureType != state.FailureMissedHeartbeat {
		t.Fatalf("expected missed_heartbeat, got %s", result.FailureType)
	}
	if result.Severity != state.SeverityCritical {
		t.Fatalf("expected critical severity, got %s", result.Severity)
	}
	if result.WorkerID != "worker-lost" {
		t.Fatalf("wrong worker_id: %s", result.WorkerID)
	}
	if result.CorrelationID != "corr-hb" {
		t.Fatalf("wrong correlation_id: %s", result.CorrelationID)
	}
}

func sample(ts time.Time, temp float64, modelHealthy bool) state.TelemetrySample {
	return state.TelemetrySample{
		WorkerID:           "worker-a",
		Timestamp:          ts,
		VRAMUsedBytes:      40,
		VRAMTotalBytes:     100,
		TemperatureCelsius: temp,
		ModelServerHealthy: modelHealthy,
	}
}
