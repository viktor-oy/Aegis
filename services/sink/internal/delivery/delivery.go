package delivery

import (
	"context"
	"errors"
	"fmt"
	"github.com/aegis/aegis/services/sink/internal/types"
	"sync"
	"time"
)

const (
	TopicGenerated = "aegis.postmortem.generated"
	TopicStatus    = "aegis.postmortem.delivery.status"
	TopicRetry     = "aegis.postmortem.delivery.retry"
	TopicDLQ       = "aegis.postmortem.delivery.dlq"
)

type Sink interface {
	Name() string
	Deliver(ctx context.Context, postmortem types.Postmortem) (string, error)
}

type StatusPublisher interface {
	PublishStatus(ctx context.Context, topic string, result types.Result) error
}

type Worker struct {
	sinks       []Sink
	publisher   StatusPublisher
	maxAttempts int
	seen        map[string]bool
	mu          sync.Mutex
}

func NewWorker(sinks []Sink, publisher StatusPublisher, maxAttempts int) *Worker {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	return &Worker{
		sinks:       sinks,
		publisher:   publisher,
		maxAttempts: maxAttempts,
		seen:        map[string]bool{},
	}
}

func (w *Worker) Deliver(ctx context.Context, postmortem types.Postmortem) ([]types.Result, error) {
	if postmortem.IncidentID == "" || postmortem.WorkerID == "" {
		return nil, errors.New("postmortem requires incident_id and worker_id")
	}
	w.mu.Lock()
	if w.seen[postmortem.IncidentID] {
		w.mu.Unlock()
		return []types.Result{{
			IncidentID:    postmortem.IncidentID,
			Status:        "duplicate_skipped",
			CorrelationID: postmortem.CorrelationID,
			CompletedAt:   time.Now().UTC(),
		}}, nil
	}
	w.seen[postmortem.IncidentID] = true
	w.mu.Unlock()

	results := make([]types.Result, 0, len(w.sinks))
	for _, sink := range w.sinks {
		result := w.deliverOne(ctx, sink, postmortem)
		results = append(results, result)
		topic := TopicStatus
		if result.Status == "dlq" {
			topic = TopicDLQ
		}
		if err := w.publisher.PublishStatus(ctx, topic, result); err != nil {
			return results, fmt.Errorf("publish delivery status: %w", err)
		}
	}
	return results, nil
}

func (w *Worker) deliverOne(ctx context.Context, sink Sink, postmortem types.Postmortem) types.Result {
	result := types.Result{
		IncidentID:    postmortem.IncidentID,
		Sink:          sink.Name(),
		CorrelationID: postmortem.CorrelationID,
		CompletedAt:   time.Now().UTC(),
	}
	var lastErr error
	for attempt := 1; attempt <= w.maxAttempts; attempt++ {
		archiveRef, err := sink.Deliver(ctx, postmortem)
		result.Attempts = attempt
		if err == nil {
			result.Status = "delivered"
			result.ArchiveRef = archiveRef
			return result
		}
		lastErr = err
	}
	result.Status = "dlq"
	result.Error = lastErr.Error()
	return result
}

type MemoryStatusPublisher struct {
	mu      sync.Mutex
	Records []PublishedStatus
}

type PublishedStatus struct {
	Topic  string
	Result types.Result
}

func (p *MemoryStatusPublisher) PublishStatus(_ context.Context, topic string, result types.Result) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Records = append(p.Records, PublishedStatus{Topic: topic, Result: result})
	return nil
}
