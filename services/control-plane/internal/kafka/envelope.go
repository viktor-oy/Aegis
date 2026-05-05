package kafka

import (
	"context"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/state"
)

const SchemaVersion = "aegis.events.v1"

type Publisher interface {
	Publish(ctx context.Context, topic string, envelope state.EventEnvelope) error
}

func NewEnvelope(eventType string, incident state.Incident, producer string, causationID string, payload map[string]any, now time.Time) state.EventEnvelope {
	return state.EventEnvelope{
		EventID:       state.NewEventID(),
		EventType:     eventType,
		IncidentID:    incident.IncidentID,
		WorkerID:      incident.WorkerID,
		Producer:      producer,
		Timestamp:     now.UTC(),
		SchemaVersion: SchemaVersion,
		CorrelationID: incident.CorrelationID,
		CausationID:   causationID,
		Payload:       payload,
	}
}
