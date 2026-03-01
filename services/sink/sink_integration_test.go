//go:build integration
// +build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aegis/aegis/services/sink/internal/delivery"
	"github.com/aegis/aegis/tests/integration/testutils"
	segmentiokafka "github.com/segmentio/kafka-go"
)

var (
	kafkaBrokers   = []string{"localhost:9094"}
	sinkBinaryPath string
)

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "sink-test-")
	if err != nil {
		fmt.Printf("failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	sinkBinaryPath = filepath.Join(tmpDir, "sink-bin")
	buildCmd := exec.Command("go", "build", "-o", sinkBinaryPath, ".")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		fmt.Printf("failed to build sink binary: %v\n%s\n", err, out)
		os.Exit(1)
	}

	code := m.Run()
	os.Exit(code)
}



type Postmortem struct {
	IncidentID    string            `json:"IncidentID"`
	WorkerID      string            `json:"WorkerID"`
	Markdown      string            `json:"Markdown"`
	Severity      string            `json:"Severity"`
	GeneratedAt   time.Time         `json:"GeneratedAt"`
	CorrelationID string            `json:"CorrelationID"`
	Metadata      map[string]string `json:"Metadata"`
}

// publishKafkaEvent is a DRY helper to publish a Postmortem event to Kafka
func publishKafkaEvent(t *testing.T, pm Postmortem) {
	t.Helper()
	w := &segmentiokafka.Writer{
		Addr:                   segmentiokafka.TCP(kafkaBrokers[0]),
		Topic:                  "aegis.postmortem.generated",
		AllowAutoTopicCreation: true,
	}
	defer w.Close()
	testutils.LogInfo(t, "📤 Publishing mock Postmortem event to Kafka (Incident: %s, Worker: %s)...", pm.IncidentID, pm.WorkerID)

	env := Envelope{
		EventID:       fmt.Sprintf("evt-%s", pm.IncidentID),
		EventType:     "aegis.postmortem.generated",
		IncidentID:    pm.IncidentID,
		WorkerID:      pm.WorkerID,
		CorrelationID: fmt.Sprintf("corr-%s", pm.IncidentID),
		Timestamp:     time.Now().Format(time.RFC3339Nano),
		Payload:       delivery.Postmortem{
			IncidentID:    pm.IncidentID,
			WorkerID:      pm.WorkerID,
			Markdown:      pm.Markdown,
			Severity:      pm.Severity,
			GeneratedAt:   pm.GeneratedAt,
			CorrelationID: pm.CorrelationID,
			Metadata:      pm.Metadata,
		},
	}

	body, _ := json.Marshal(env)
	var err error
	for i := 0; i < 5; i++ {
		writeCtx, cancelWrite := context.WithTimeout(context.Background(), 10*time.Second)
		err = w.WriteMessages(writeCtx, segmentiokafka.Message{
			Key:   []byte(pm.WorkerID),
			Value: body,
		})
		cancelWrite()
		if err == nil {
			testutils.LogInfo(t, "✅ Successfully published mock Postmortem event to Kafka")
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		t.Fatalf("Failed to publish to kafka: %v", err)
	}
}

func TestSinkServiceIntegration_FileSink(t *testing.T) {
	testutils.WipeTestState(t, "kafka:aegis.postmortem.generated,aegis.postmortem.delivery.status")
	// Create a random test directory within docs/services/sink/local/artifacts
	docsDir := "../../docs/services/sink/local/artifacts"
	os.MkdirAll(docsDir, 0755)

	// Start the sink service via compiled binary
	t.Log("Starting sink service via compiled binary...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, sinkBinaryPath)
	defer func() {
		cancel()
		cmd.Wait() // wait for process to die completely
	}()
	cmd.Env = append(os.Environ(),
		"AEGIS_DEBUG=true",
		"AEGIS_KAFKA_BROKERS="+kafkaBrokers[0],
		"AEGIS_KAFKA_GROUP_ID=test-group-file-sink",
		"AEGIS_FILE_SINK_ENABLED=true",
		"AEGIS_FILE_SINK_PATH="+docsDir,
	)
	
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf
	
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start sink service: %v", err)
	}

	// Wait for the HTTP server to be ready
	time.Sleep(3 * time.Second)

	pm := Postmortem{
		IncidentID: "integ-test-incident-kafka-" + time.Now().Format("150405.000000"),
		WorkerID:   "integ-gpu-worker-1",
		Markdown:   "# Kafka Integration Test Postmortem",
		Severity:   "high",
	}

	// We test via Kafka by publishing an event
	publishKafkaEvent(t, pm)

	testutils.LogInfo(t, "🔍 Waiting and verifying file sink creation in %s...", docsDir)

	// Verify that a file was created in docs/services/sink/local/artifacts
	var found bool
	for i := 0; i < 50; i++ {
		entries, _ := os.ReadDir(docsDir)
		for _, entry := range entries {
			if strings.Contains(entry.Name(), pm.IncidentID) {
				found = true
				content, _ := os.ReadFile(filepath.Join(docsDir, entry.Name()))
				if !strings.Contains(string(content), pm.Markdown) {
					t.Errorf("File %s does not contain the expected markdown\nService Output:\n%s", entry.Name(), outBuf.String())
				}
				t.Logf("Found postmortem file via Kafka: %s", entry.Name())
				break
			}
		}
		if found {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !found {
		t.Errorf("Expected to find file containing %s, but found none in %s\nService Output:\n%s", pm.IncidentID, docsDir, outBuf.String())
	}
}

func TestSinkServiceIntegration_EmailSink(t *testing.T) {
	testutils.WipeTestState(t, "kafka:aegis.postmortem.generated,aegis.postmortem.delivery.status")
	pm := Postmortem{
		IncidentID:  "email-integ-123",
		WorkerID:    "gpu-01",
		Severity:    "high",
		Markdown:    "# Incident 123\nDetails here.",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, sinkBinaryPath)
	defer func() {
		cancel()
		cmd.Wait() // wait for process to die completely
	}()
	cmd.Env = append(os.Environ(),
		"AEGIS_DEBUG=true",
		"AEGIS_KAFKA_BROKERS="+kafkaBrokers[0],
		"AEGIS_KAFKA_GROUP_ID=test-group-email-sink",
		"AEGIS_EMAIL_SINK_ENABLED=true",
		"AEGIS_SMTP_HOST=127.0.0.1",
		"AEGIS_SMTP_PORT=10250",
		"AEGIS_SMTP_TO=alerts@aegis.local",
		"AEGIS_SMTP_FROM=noreply@aegis.local",
	)

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start sink service: %v", err)
	}

	// Wait a moment for service to start
	time.Sleep(3 * time.Second)

	testutils.LogInfo(t, "🔍 Waiting and verifying email sink delivery via Mailpit...")

	// Publish message to Kafka to trigger email
	publishKafkaEvent(t, pm)

	// Wait for processing to complete and query Mailpit
	var found bool
	for i := 0; i < 50; i++ {
		resp, err := http.Get("http://127.0.0.1:8025/api/v1/messages")
		if err == nil && resp.StatusCode == 200 {
			var result struct {
				Messages []struct {
					Subject string `json:"Subject"`
					ID      string `json:"ID"`
				} `json:"messages"`
			}
			json.NewDecoder(resp.Body).Decode(&result)
			resp.Body.Close()

			for _, msg := range result.Messages {
				if strings.Contains(msg.Subject, pm.IncidentID) {
					// We found the email, let's verify the body
					msgResp, err := http.Get("http://127.0.0.1:8025/api/v1/message/" + msg.ID)
					if err == nil && msgResp.StatusCode == 200 {
						var msgDetail struct {
							Text string `json:"Text"`
						}
						json.NewDecoder(msgResp.Body).Decode(&msgDetail)
						msgResp.Body.Close()

						normalizedCapturedMsg := strings.ReplaceAll(msgDetail.Text, "\r\n", "\n")
						if strings.Contains(normalizedCapturedMsg, pm.Markdown) {
							found = true
							break
						}
					}
				}
			}
		}
		if found {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !found {
		t.Fatalf("Email was never sent or didn't match expected content.\nService Output:\n%s", outBuf.String())
	}
}

func TestSinkServiceIntegration_HTTP(t *testing.T) {
	t.Skip("HTTP Sink is not implemented in main.go")
	testutils.WipeTestState(t, "kafka:aegis.postmortem.generated,aegis.postmortem.delivery.status")
	// Start the sink service via compiled binary
	t.Log("Starting sink service via compiled binary for HTTP test...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cmd := exec.CommandContext(ctx, sinkBinaryPath)
	defer func() {
		cancel()
		cmd.Wait() // wait for process to die completely
	}()
	cmd.Env = append(os.Environ(),
		"AEGIS_DEBUG=true",
		"AEGIS_KAFKA_BROKERS="+kafkaBrokers[0],
		"AEGIS_KAFKA_GROUP_ID=test-group-http-sink",
		"AEGIS_HTTP_SINK_ENABLED=true",
	)
	
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf
	
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start sink service: %v", err)
	}

	// Wait for the HTTP server to be ready
	time.Sleep(3 * time.Second)

	testutils.LogInfo(t, "🔍 Waiting and verifying HTTP sink delivery to mock server...")

	// We test via HTTP test-delivery endpoint
	pm := Postmortem{
		IncidentID: "integ-test-incident-http-" + time.Now().Format("150405.000000"),
		WorkerID:   "integ-gpu-worker-1",
		Markdown:   "# HTTP Integration Test Postmortem",
		Severity:   "high",
	}

	body, _ := json.Marshal(pm)
	resp, err := http.Post("http://localhost:8081/test-delivery", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("Failed to call test-delivery endpoint: %v\nService Output:\n%s", err, outBuf.String())
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("Expected 200 OK from test-delivery, got %d. Body: %s\nService Output:\n%s", resp.StatusCode, string(respBody), outBuf.String())
	}
}
