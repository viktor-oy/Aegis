package state

import (
	"crypto/rand"
	"encoding/hex"
	"time"
)

type WorkerHealthState string

const (
	WorkerHealthy              WorkerHealthState = "HEALTHY"
	WorkerSuspected            WorkerHealthState = "SUSPECTED"
	WorkerDiagnosticsTriggered WorkerHealthState = "DIAGNOSTICS_TRIGGERED"
	WorkerDiagnosticsCollected WorkerHealthState = "DIAGNOSTICS_COLLECTED"
	WorkerPostmortemRequested  WorkerHealthState = "POSTMORTEM_REQUESTED"
	WorkerPostmortemGenerated  WorkerHealthState = "POSTMORTEM_GENERATED"
	WorkerDeliveryInProgress   WorkerHealthState = "DELIVERY_IN_PROGRESS"
	WorkerDelivered            WorkerHealthState = "DELIVERED"
	WorkerDeliveryFailed       WorkerHealthState = "DELIVERY_FAILED"
	WorkerResolved             WorkerHealthState = "RESOLVED"
)

type FailureType string

func CreateProducerRef(id string) string {
	return "aegis-control-plane/" + id
}

const (
	FailureMissedHeartbeat FailureType = "missed_heartbeat"
	FailureGPUOverheat     FailureType = "gpu_overheat"
	FailureVRAMPressure    FailureType = "vram_pressure"
	FailureECCBurst        FailureType = "ecc_burst"
	FailureModelUnhealthy  FailureType = "model_unhealthy"
	FailureLatencySpike    FailureType = "latency_spike"
	FailureSynthetic       FailureType = "synthetic_failure"
)

type Severity string

const (
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

type TelemetrySample struct {
	WorkerID             string
	Timestamp            time.Time
	ReceivedAt           time.Time
	GPUUtilization       float64
	VRAMUsedBytes        uint64
	VRAMTotalBytes       uint64
	TemperatureCelsius   float64
	PowerWatts           float64
	ECCErrorCount        uint64
	InferenceLatencyMS   float64
	LocalQueueDepth      uint64
	ModelServerHealthy   bool
	SyntheticFailureFlag string
	CorrelationID        string
}

func (s TelemetrySample) VRAMRatio() float64 {
	if s.VRAMTotalBytes == 0 {
		return 0
	}
	return float64(s.VRAMUsedBytes) / float64(s.VRAMTotalBytes)
}

type DetectionResult struct {
	WorkerID      string
	FailureType   FailureType
	Severity      Severity
	Reason        string
	ObservedAt    time.Time
	CorrelationID string
}

type Incident struct {
	IncidentID    string
	WorkerID      string
	FailureType   FailureType
	State         WorkerHealthState
	Severity      Severity
	DetectedAt    time.Time
	CorrelationID string
}

type DiagnosticRequest struct {
	WorkerID      string
	IncidentID    string
	FailureType   FailureType
	RequestedAt   time.Time
	CorrelationID string
}

type DiagnosticBundle struct {
	WorkerID         string
	IncidentID       string
	CollectedAt      time.Time
	DiagnosticStatus string
	Payload          map[string]any
	CorrelationID    string
}

type EventEnvelope struct {
	EventID       string         `json:"event_id"`
	EventType     string         `json:"event_type"`
	IncidentID    string         `json:"incident_id"`
	WorkerID      string         `json:"worker_id"`
	Producer      string         `json:"producer"`
	Timestamp     time.Time      `json:"timestamp"`
	SchemaVersion string         `json:"schema_version"`
	CorrelationID string         `json:"correlation_id"`
	CausationID   string         `json:"causation_id"`
	Payload       map[string]any `json:"payload"`
}

func NewEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return hex.EncodeToString(b[:])
}
