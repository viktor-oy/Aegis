package server

import (
	"context"
	"testing"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type staticRing struct {
	member hashring.Member
}

func (r staticRing) Owner(string) (hashring.Member, bool) { return r.member, true }

type okDiagnostics struct{}

func (okDiagnostics) TriggerDiagnostics(_ context.Context, req state.DiagnosticRequest) (state.DiagnosticBundle, error) {
	return state.DiagnosticBundle{
		WorkerID:         req.WorkerID,
		IncidentID:       req.IncidentID,
		CollectedAt:      req.RequestedAt,
		DiagnosticStatus: "complete",
		Payload:          map[string]any{"ok": true},
		CorrelationID:    req.CorrelationID,
	}, nil
}

func TestOwnerRedirect(t *testing.T) {
	cp := New("cp-a", "a:50051", staticRing{member: hashring.Member{ID: "cp-b", Address: "b:50051"}}, nil, 1, time.Second)
	directive, err := cp.Ingest(context.Background(), state.TelemetrySample{WorkerID: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	if directive.Type != "redirect" || directive.OwnerHint != "b:50051" {
		t.Fatalf("unexpected directive %#v", directive)
	}
}

func TestResourceExhausted(t *testing.T) {
	store := membership.NewInMemoryStore()
	manager := incident.NewManager(store, &kafka.MemoryPublisher{}, okDiagnostics{}, "cp-a")
	cp := New("cp-a", "a:50051", staticRing{member: hashring.Member{ID: "cp-a", Address: "a:50051"}}, manager, 1, time.Second)
	sample := state.TelemetrySample{WorkerID: "worker-a", Timestamp: time.Now(), ModelServerHealthy: true}
	if _, err := cp.Ingest(context.Background(), sample); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.Ingest(context.Background(), sample); err != ErrResourceExhausted {
		t.Fatalf("expected resource exhausted, got %v", err)
	}
}

