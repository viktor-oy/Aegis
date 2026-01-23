package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/aegis/aegis/services/sink-workers/internal/delivery"
	"github.com/aegis/aegis/services/sink-workers/internal/sinks"
)

func main() {
	pub := &delivery.MemoryStatusPublisher{}
	worker := delivery.NewWorker([]delivery.Sink{
		&sinks.SlackSink{WebhookURL: os.Getenv("AEGIS_SLACK_WEBHOOK_URL")},
		&sinks.PostgresSink{DSN: os.Getenv("AEGIS_POSTGRES_DSN")},
		&sinks.S3Sink{Bucket: getenv("AEGIS_S3_BUCKET", "aegis-postmortems")},
	}, pub, 3)

	postmortem := delivery.Postmortem{
		IncidentID:    "bootstrap-self-test",
		WorkerID:      "sink-worker",
		Markdown:      "# bootstrap self test\n",
		Severity:      "info",
		GeneratedAt:   time.Now().UTC(),
		CorrelationID: "sink-worker-startup",
	}
	results, err := worker.Deliver(context.Background(), postmortem)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("sink worker ready; bootstrap delivery statuses=%v", results)
	select {}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

