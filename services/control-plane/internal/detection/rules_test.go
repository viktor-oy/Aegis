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

