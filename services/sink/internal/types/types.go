package types

import "time"

type Postmortem struct {
	IncidentID    string            `json:"incident_id"`
	WorkerID      string            `json:"worker_id"`
	Markdown      string            `json:"markdown"`
	Severity      string            `json:"severity"`
	GeneratedAt   time.Time         `json:"generated_at"`
	CorrelationID string            `json:"correlation_id"`
	Metadata      map[string]string `json:"metadata"`
}

type Result struct {
	IncidentID    string    `json:"incident_id"`
	Sink          string    `json:"sink"`
	Status        string    `json:"status"`
	Attempts      int       `json:"attempts"`
	Error         string    `json:"error,omitempty"`
	ArchiveRef    string    `json:"archive_ref,omitempty"`
	CorrelationID string    `json:"correlation_id"`
	CompletedAt   time.Time `json:"completed_at"`
}

// Envelope represents the common event structure in Kafka
type Envelope struct {
	EventID       string     `json:"event_id"`
	EventType     string     `json:"event_type"`
	IncidentID    string     `json:"incident_id"`
	WorkerID      string     `json:"worker_id"`
	CorrelationID string     `json:"correlation_id"`
	Timestamp     string     `json:"timestamp"`
	Payload       Postmortem `json:"payload"`
}
