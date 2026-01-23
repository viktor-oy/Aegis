package delivery

import (
	"context"
	"errors"
	"testing"
)

type stubSink struct {
	name      string
	failures  int
	delivered int
}

func (s *stubSink) Name() string { return s.name }

func (s *stubSink) Deliver(context.Context, Postmortem) (string, error) {
	s.delivered++
	if s.delivered <= s.failures {
		return "", errors.New("temporary failure")
	}
	return "ok://" + s.name, nil
}

func TestWorkerRetriesThenDelivers(t *testing.T) {
	pub := &MemoryStatusPublisher{}
	sink := &stubSink{name: "slack", failures: 1}
	worker := NewWorker([]Sink{sink}, pub, 3)
	results, err := worker.Deliver(context.Background(), Postmortem{IncidentID: "inc-1", WorkerID: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != "delivered" || results[0].Attempts != 2 {
		t.Fatalf("unexpected result %#v", results[0])
	}
	if pub.Records[0].Topic != TopicStatus {
		t.Fatalf("unexpected topic %s", pub.Records[0].Topic)
	}
}

func TestWorkerMovesPermanentFailureToDLQ(t *testing.T) {
	pub := &MemoryStatusPublisher{}
	sink := &stubSink{name: "s3", failures: 5}
	worker := NewWorker([]Sink{sink}, pub, 2)
	results, err := worker.Deliver(context.Background(), Postmortem{IncidentID: "inc-1", WorkerID: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Status != "dlq" {
		t.Fatalf("expected dlq, got %#v", results[0])
	}
	if pub.Records[0].Topic != TopicDLQ {
		t.Fatalf("unexpected topic %s", pub.Records[0].Topic)
	}
}

