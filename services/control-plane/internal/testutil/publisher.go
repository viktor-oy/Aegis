package testutil

import (
	"context"
	"sync"

	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type PublishedEvent struct {
	Topic    string
	Envelope state.EventEnvelope
}

type MockPublisher struct {
	mu     sync.Mutex
	events []PublishedEvent
}

func (p *MockPublisher) Publish(_ context.Context, topic string, envelope state.EventEnvelope) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, PublishedEvent{Topic: topic, Envelope: envelope})
	return nil
}

func (p *MockPublisher) Events() []PublishedEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]PublishedEvent(nil), p.events...)
}
