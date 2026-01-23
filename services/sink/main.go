package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aegis/aegis/services/sink/internal/delivery"
	aegiskafka "github.com/aegis/aegis/services/sink/internal/kafka"
	"github.com/aegis/aegis/services/sink/internal/sinks"
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

func setupLogger(debug bool) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	opts := &slog.HandlerOptions{Level: level}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	return slog.New(handler)
}

func main() {
	cfg := loadConfig()
	logger := setupLogger(cfg.Debug)
	slog.SetDefault(logger)

	var activeSinks []delivery.Sink

	if cfg.FileSinkEnabled {
		logger.Info("File sink enabled", "directory", cfg.FileSinkDir)
		activeSinks = append(activeSinks, &sinks.FileSink{
			Directory: cfg.FileSinkDir,
			Logger:    logger,
		})
	}

	if cfg.EmailSinkEnabled {
		logger.Info("Email sink enabled")
		emailSink := &sinks.EmailSink{
			Host:     cfg.SMTPHost,
			Port:     cfg.SMTPPort,
			Username: cfg.SMTPUser,
			Password: cfg.SMTPPass,
			To:       cfg.SMTPTo,
			From:     cfg.SMTPFrom,
			Logger:   logger,
		}
		if err := emailSink.Validate(); err != nil {
			logger.Error("Email sink configuration error", "error", err)
			os.Exit(1)
		}
		activeSinks = append(activeSinks, emailSink)
	}

	if len(activeSinks) == 0 {
		logger.Warn("No sinks are enabled. Sink service will consume messages but not deliver them.")
	}

	publisher := aegiskafka.NewPublisher(cfg.KafkaBrokers, logger)
	defer publisher.Close()

	worker := delivery.NewWorker(activeSinks, publisher, 3)

	if cfg.Debug {
		go startHTTPServer(logger, worker)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		Topic:   delivery.TopicGenerated,
		GroupID: cfg.KafkaGroupID,
	})
	defer reader.Close()

	go consumeKafka(ctx, logger, reader, worker)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	logger.Info("Received signal, shutting down", "signal", sig)
}

// Envelope represents the common event structure in Kafka
type Envelope struct {
	EventID       string              `json:"event_id"`
	EventType     string              `json:"event_type"`
	IncidentID    string              `json:"incident_id"`
	WorkerID      string              `json:"worker_id"`
	CorrelationID string              `json:"correlation_id"`
	Timestamp     string              `json:"timestamp"`
	Payload       delivery.Postmortem `json:"payload"`
}

func consumeKafka(ctx context.Context, logger *slog.Logger, reader *kafka.Reader, worker *delivery.Worker) {
	logger.Info("Started consuming from Kafka", "topic", delivery.TopicGenerated)

	for {
		m, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			logger.Error("Error reading message from kafka", "error", err)
			continue
		}

		var envelope Envelope
		if err := json.Unmarshal(m.Value, &envelope); err != nil {
			logger.Error("Failed to unmarshal kafka message envelope", "error", err, "message", string(m.Value))
			continue
		}

		if envelope.EventType != "aegis.postmortem.generated" {
			logger.Warn("Ignored unexpected event type", "event_type", envelope.EventType)
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

		results, err := worker.Deliver(ctx, pm)
		if err != nil {
			logger.Error("Error during delivery", "incident_id", pm.IncidentID, "error", err)
			continue
		}

		logger.Info("Processed generated postmortem", "incident_id", pm.IncidentID, "results", results)
	}
}

// HTTP Server logic for swagger / test trigger
func startHTTPServer(logger *slog.Logger, worker *delivery.Worker) {
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
		var pm delivery.Postmortem
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

	logger.Info("Starting debug HTTP server on :8081")
	if err := http.ListenAndServe(":8081", mux); err != nil {
		logger.Error("HTTP server error", "error", err)
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
