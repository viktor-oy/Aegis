package delivery

import (
	"context"
	"errors"
	"testing"
)

type mockSink struct {
	name        string
	deliverFunc func(ctx context.Context, postmortem Postmortem) (string, error)
}

func (m *mockSink) Name() string { return m.name }
func (m *mockSink) Deliver(ctx context.Context, postmortem Postmortem) (string, error) {
	return m.deliverFunc(ctx, postmortem)
}

func TestWorker_Deliver(t *testing.T) {
	sink1 := &mockSink{
		name: "sink1",
		deliverFunc: func(ctx context.Context, postmortem Postmortem) (string, error) {
			return "ref1", nil
		},
	}
	sink2 := &mockSink{
		name: "sink2",
		deliverFunc: func(ctx context.Context, postmortem Postmortem) (string, error) {
			return "", errors.New("fail")
		},
	}

	pub := &MemoryStatusPublisher{}
	worker := NewWorker([]Sink{sink1, sink2}, pub, 2)

	pm := Postmortem{
		IncidentID: "test-incident",
		WorkerID:   "aegis-system--test-worker",
	}

	results, err := worker.Deliver(context.Background(), pm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].Status != "delivered" {
		t.Errorf("expected delivered, got %s", results[0].Status)
	}

	if results[1].Status != "dlq" {
		t.Errorf("expected dlq, got %s", results[1].Status)
	}
	if results[1].Attempts != 2 {
		t.Errorf("expected 2 attempts for sink2, got %d", results[1].Attempts)
	}

	if len(pub.Records) != 2 {
		t.Fatalf("expected 2 published statuses, got %d", len(pub.Records))
	}
}
