package sinks

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/aegis/aegis/services/sink-workers/internal/delivery"
)

type SlackSink struct {
	WebhookURL string
	Messages   []string
	mu         sync.Mutex
}

func (s *SlackSink) Name() string { return "slack" }

func (s *SlackSink) Deliver(_ context.Context, postmortem delivery.Postmortem) (string, error) {
	if s.WebhookURL == "" {
		return "", errors.New("slack webhook url is not configured")
	}
	summary := fmt.Sprintf("[%s] incident %s on %s", postmortem.Severity, postmortem.IncidentID, postmortem.WorkerID)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Messages = append(s.Messages, summary)
	return "slack://message/" + postmortem.IncidentID, nil
}

type PostgresSink struct {
	DSN     string
	Records map[string]delivery.Postmortem
	mu      sync.Mutex
}

func (s *PostgresSink) Name() string { return "postgresql" }

func (s *PostgresSink) Deliver(_ context.Context, postmortem delivery.Postmortem) (string, error) {
	if s.DSN == "" {
		return "", errors.New("postgres dsn is not configured")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Records == nil {
		s.Records = map[string]delivery.Postmortem{}
	}
	s.Records[postmortem.IncidentID] = postmortem
	return "postgresql://incidents/" + postmortem.IncidentID, nil
}

type S3Sink struct {
	Bucket  string
	Objects map[string]string
	mu      sync.Mutex
}

func (s *S3Sink) Name() string { return "s3" }

func (s *S3Sink) Deliver(_ context.Context, postmortem delivery.Postmortem) (string, error) {
	if s.Bucket == "" {
		return "", errors.New("s3 bucket is not configured")
	}
	key := ArchiveKey(postmortem.WorkerID, postmortem.IncidentID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Objects == nil {
		s.Objects = map[string]string{}
	}
	s.Objects[key] = postmortem.Markdown
	return "s3://" + s.Bucket + "/" + key, nil
}

func ArchiveKey(workerID, incidentID string) string {
	cleanWorker := strings.ReplaceAll(workerID, "/", "_")
	cleanIncident := strings.ReplaceAll(incidentID, "/", "_")
	return fmt.Sprintf("postmortems/%s/%s/postmortem.md", cleanWorker, cleanIncident)
}

