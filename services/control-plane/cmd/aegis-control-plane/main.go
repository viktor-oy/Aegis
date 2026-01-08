package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aegis/aegis/services/control-plane/internal/hashring"
	"github.com/aegis/aegis/services/control-plane/internal/incident"
	"github.com/aegis/aegis/services/control-plane/internal/kafka"
	"github.com/aegis/aegis/services/control-plane/internal/membership"
	"github.com/aegis/aegis/services/control-plane/internal/server"
	"github.com/aegis/aegis/services/control-plane/internal/state"
)

type localDiagnostics struct{}

func (localDiagnostics) TriggerDiagnostics(_ context.Context, req state.DiagnosticRequest) (state.DiagnosticBundle, error) {
	return state.DiagnosticBundle{
		WorkerID:         req.WorkerID,
		IncidentID:       req.IncidentID,
		CollectedAt:      time.Now().UTC(),
		DiagnosticStatus: "partial",
		Payload: map[string]any{
			"note": "local MVP diagnostic adapter; gRPC TriggerDiagnostics is wired by deployment adapter",
		},
		CorrelationID: req.CorrelationID,
	}, nil
}

func main() {
	id := getenv("AEGIS_CP_ID", "cp-0")
	address := getenv("AEGIS_CP_ADDRESS", "aegis-control-plane:50051")
	httpAddress := getenv("AEGIS_HTTP_ADDRESS", ":8080")

	store := membership.NewInMemoryStore()
	member := hashring.Member{ID: id, Address: address}
	now := time.Now().UTC()
	if err := store.Register(context.Background(), member, 30*time.Second, now); err != nil {
		log.Fatal(err)
	}
	members, _ := store.ActiveMembers(context.Background(), now)
	ring, err := hashring.New(members, 128)
	if err != nil {
		log.Fatal(err)
	}
	publisher := &kafka.MemoryPublisher{}
	manager := incident.NewManager(store, publisher, localDiagnostics{}, "aegis-control-plane/"+id)
	cp := server.New(id, address, ring, manager, 256, 15*time.Second)

	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, "ok queue_depth=%d\n", cp.QueueDepth())
	})
	http.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, "ready")
	})

	log.Printf("aegis control-plane %s listening on %s", id, httpAddress)
	log.Fatal(http.ListenAndServe(httpAddress, nil))
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

