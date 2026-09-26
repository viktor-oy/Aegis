package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aegis/aegis/services/sink/internal/types"

	"github.com/aegis/aegis/services/sink/internal/delivery"
	aegiskafka "github.com/aegis/aegis/services/sink/internal/kafka"
	"github.com/aegis/aegis/services/sink/internal/sinks"
	"github.com/aegis/aegis/services/pkg/logger"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	Debug            bool
	KafkaBrokers     []string
	KafkaGroupID     string
	FileSinkEnabled  bool
	FileSinkDir      string
	EmailSinkEnabled bool
	SMTPHost         string
	SMTPPort         string
	SMTPUser         string
	SMTPPass         string
	SMTPTo           string
	SMTPFrom         string
}

func loadConfig() Config {
	return Config{
		Debug:            os.Getenv("AEGIS_DEBUG") == "true",
		KafkaBrokers:     strings.Split(getEnv("AEGIS_KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaGroupID:     getEnv("AEGIS_KAFKA_GROUP_ID", "aegis-sink-workers"),
		FileSinkEnabled:  os.Getenv("AEGIS_FILE_SINK_ENABLED") == "true",
		FileSinkDir:      getEnv("AEGIS_FILE_SINK_PATH", "/tmp/aegis-postmortems"),
		EmailSinkEnabled: os.Getenv("AEGIS_EMAIL_SINK_ENABLED") == "true",
		SMTPHost:         os.Getenv("AEGIS_SMTP_HOST"),
		SMTPPort:         os.Getenv("AEGIS_SMTP_PORT"),
		SMTPUser:         os.Getenv("AEGIS_SMTP_USER"),
		SMTPPass:         os.Getenv("AEGIS_SMTP_PASS"),
		SMTPTo:           os.Getenv("AEGIS_SMTP_TO"),
		SMTPFrom:         os.Getenv("AEGIS_SMTP_FROM"),
	}
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func main() {
	cfg := loadConfig()
	logger.Setup(cfg.Debug)
	
	var activeSinks []delivery.Sink

	if cfg.FileSinkEnabled {
		slog.Info("File sink enabled", "component", "MAIN", "event", "FILE_SINK_INIT", "directory", cfg.FileSinkDir)
		activeSinks = append(activeSinks, &sinks.FileSink{
			Directory: cfg.FileSinkDir,
		})
	}

	if cfg.EmailSinkEnabled {
		slog.Info("Email sink enabled", "component", "MAIN", "event", "EMAIL_SINK_INIT")
		emailSink := &sinks.EmailSink{
			Host:     cfg.SMTPHost,
			Port:     cfg.SMTPPort,
			Username: cfg.SMTPUser,
			Password: cfg.SMTPPass,
			To:       cfg.SMTPTo,
			From:     cfg.SMTPFrom,
		}
		if err := emailSink.Validate(); err != nil {
			slog.Error("Email sink configuration error", "component", "MAIN", "event", "EMAIL_SINK_ERR", "error", err)
			os.Exit(1)
		}
		activeSinks = append(activeSinks, emailSink)
	}

	if len(activeSinks) == 0 {
		slog.Warn("No sinks are enabled. Sink service will consume messages but not deliver them.", "component", "MAIN", "event", "NO_SINKS")
	}

	publisher := aegiskafka.NewPublisher(cfg.KafkaBrokers)
	defer publisher.Close()

	worker := delivery.NewWorker(activeSinks, publisher, 3)

	if cfg.Debug {
		go startHTTPServer(worker)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		KeepAlive: 30 * time.Second,
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		Topic:       delivery.TopicGenerated,
		GroupID:     cfg.KafkaGroupID,
		StartOffset: kafka.FirstOffset,
		Dialer:      &kafka.Dialer{
			Timeout:   dialer.Timeout,
			DualStack: dialer.DualStack,
			KeepAlive: dialer.KeepAlive,
		},
	})
	defer reader.Close()

	go consumeKafka(ctx, reader, worker)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	slog.Info("Received signal, shutting down", "component", "MAIN", "event", "SHUTDOWN", "signal", sig)
}

func consumeKafka(ctx context.Context, reader *kafka.Reader, worker *delivery.Worker) {
	slog.Info("Started consuming from Kafka", "component", "KAFKA", "event", "CONSUME_START", "topic", delivery.TopicGenerated)

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Error("Error reading message from kafka", "component", "KAFKA", "event", "READ_ERR", "error", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		var envelope types.Envelope
		if err := json.Unmarshal(m.Value, &envelope); err != nil {
			slog.Error("Failed to unmarshal kafka message envelope", "component", "KAFKA", "event", "UNMARSHAL_ERR", "error", err, "message", string(m.Value))
			_ = reader.CommitMessages(ctx, m)
			continue
		}

		if envelope.EventType != "aegis.postmortem.generated" {
			slog.Warn("Ignored unexpected event type", "component", "KAFKA", "event", "UNEXPECTED_EVENT", "event_type", envelope.EventType)
			_ = reader.CommitMessages(ctx, m)
			continue
		}

		pm := envelope.Payload
		// ensure payload has required fields from envelope if not set inside payload
		if pm.IncidentID == "" {
			pm.IncidentID = envelope.IncidentID
		}
		if pm.WorkerID == "" {
			pm.WorkerID = envelope.WorkerID
		}
		if pm.CorrelationID == "" {
			pm.CorrelationID = envelope.CorrelationID
		}

		for {
			results, err := worker.Deliver(ctx, pm)
			if err != nil {
				slog.Error("Error during delivery. Retrying in 5s...", "component", "SINK_DELIVERY", "event", "DELIVERY_ERR", "incident_id", pm.IncidentID, "error", err)
				time.Sleep(5 * time.Second)
				continue
			}
			slog.Info("Processed generated postmortem", "component", "SINK_DELIVERY", "event", "DELIVERY_SUCCESS", "incident_id", pm.IncidentID, "results", results)
			break
		}
		
		if err := reader.CommitMessages(ctx, m); err != nil {
			slog.Error("Failed to commit kafka offset", "component", "KAFKA", "event", "COMMIT_ERR", "error", err)
		}
	}
}

// HTTP Server logic for swagger / test trigger
func startHTTPServer(worker *delivery.Worker) {
	mux := http.NewServeMux()

	mux.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(swaggerHTML))
	})

	mux.HandleFunc("/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.Write([]byte(openAPIYaml))
	})

	mux.HandleFunc("/test-delivery", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var pm types.Postmortem
		if err := json.NewDecoder(r.Body).Decode(&pm); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		results, err := worker.Deliver(context.Background(), pm)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	})

	port := os.Getenv("AEGIS_HTTP_PORT")
	if port == "" {
		port = "8081"
	}

	slog.Info("Starting debug HTTP server", "component", "MAIN", "event", "HTTP_START", "port", port)
	if err := http.ListenAndServe(":"+port, mux); err != nil {
		slog.Error("HTTP server error", "component", "MAIN", "event", "HTTP_ERR", "error", err)
	}
}

const swaggerHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta
    name="description"
    content="SwaggerUI"
  />
  <title>Aegis Sink SwaggerUI</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui.css" />
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5.11.0/swagger-ui-bundle.js" crossorigin></script>
<script>
  window.onload = () => {
    window.ui = SwaggerUIBundle({
      url: '/openapi.yaml',
      dom_id: '#swagger-ui',
    });
  };
</script>
</body>
</html>
`

const openAPIYaml = `
openapi: 3.0.0
info:
  title: Aegis Sink API
  description: HTTP interface for testing sink delivery. (Only available in debug mode).
  version: "1.0.0"
paths:
  /test-delivery:
    post:
      summary: Test postmortem delivery
      description: Delivers a test postmortem to all enabled sinks synchronously.
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required:
                - IncidentID
                - WorkerID
                - Markdown
              properties:
                IncidentID:
                  type: string
                  example: "test-incident-123"
                WorkerID:
                  type: string
                  example: "gpu-worker-1"
                Markdown:
                  type: string
                  example: "# Test Postmortem\nThis is a test."
                Severity:
                  type: string
                  example: "info"
                CorrelationID:
                  type: string
                  example: "test-corr-123"
      responses:
        '200':
          description: Delivery results
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    IncidentID:
                      type: string
                    Sink:
                      type: string
                    Status:
                      type: string
                    Attempts:
                      type: integer
                    Error:
                      type: string
                    ArchiveRef:
                      type: string
                    CorrelationID:
                      type: string
        '400':
          description: Invalid request payload
        '500':
          description: Internal server error
`
