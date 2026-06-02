package sinks

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/smtp"
	"os"
	"path/filepath"
	"sync"

	"github.com/aegis/aegis/services/sink/internal/types"
)

type FileSink struct {
	Directory string
	mu        sync.Mutex
}

func (s *FileSink) Name() string { return "file" }

func (s *FileSink) Deliver(_ context.Context, postmortem types.Postmortem) (string, error) {
	if s.Directory == "" {
		return "", errors.New("file sink directory is not configured")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Ensure directory exists
	if err := os.MkdirAll(s.Directory, 0755); err != nil {
		return "", fmt.Errorf("failed to create directory: %w", err)
	}

	// Generate random file name as requested
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
	}
	randomName := fmt.Sprintf("incident_%s_%s.md", postmortem.IncidentID, hex.EncodeToString(b))
	filePath := filepath.Join(s.Directory, randomName)

	if err := os.WriteFile(filePath, []byte(postmortem.Markdown), 0644); err != nil {
		return "", fmt.Errorf("failed to write file: %w", err)
	}

	slog.Info("Saved postmortem to file", "component", "SINK_DELIVERY", "event", "FILE_DELIVERY_SUCCESS", "path", filePath)

	return "file://" + filePath, nil
}

type EmailSink struct {
	Host         string
	Port         string
	Username     string
	Password     string
	To           string
	From         string
	SendMailFunc func(addr string, a smtp.Auth, from string, to []string, msg []byte) error
}

func (s *EmailSink) Name() string { return "email" }

func (s *EmailSink) Validate() error {
	if s.Host == "" || s.Port == "" || s.To == "" || s.From == "" {
		return errors.New("incomplete SMTP configuration for email sink")
	}
	return nil
}

func (s *EmailSink) Deliver(_ context.Context, postmortem types.Postmortem) (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}

	subject := fmt.Sprintf("Subject: [%s] Incident %s on %s\r\n", postmortem.Severity, postmortem.IncidentID, postmortem.WorkerID)
	mime := "MIME-version: 1.0;\nContent-Type: text/plain; charset=\"UTF-8\";\n\n"
	body := subject + mime + postmortem.Markdown

	slog.Debug("Sending email", "component", "SINK_DELIVERY", "event", "EMAIL_SENDING", "to", s.To, "from", s.From, "subject", subject)

	var auth smtp.Auth
	if s.Username != "" && s.Password != "" {
		auth = smtp.PlainAuth("", s.Username, s.Password, s.Host)
	}

	sendFn := s.SendMailFunc
	if sendFn == nil {
		sendFn = smtp.SendMail
	}
	err := sendFn(s.Host+":"+s.Port, auth, s.From, []string{s.To}, []byte(body))
	if err != nil {
		return "", fmt.Errorf("failed to send email: %w", err)
	}

	return fmt.Sprintf("email://%s/%s", s.To, postmortem.IncidentID), nil
}
