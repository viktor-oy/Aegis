package sinks

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/aegis/aegis/services/sink/internal/delivery"
)

func TestFileSink_Deliver(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "aegis-filesink-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	sink := &FileSink{
		Directory: tempDir,
	}

	pm := delivery.Postmortem{
		IncidentID: "test-incident",
		Markdown:   "# Test\nSuccess",
	}

	ref, err := sink.Deliver(context.Background(), pm)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ref == "" {
		t.Fatal("expected non-empty archive ref")
	}

	// Verify a file was created
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 file, got %d", len(entries))
	}

	content, err := os.ReadFile(filepath.Join(tempDir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}

	if string(content) != pm.Markdown {
		t.Errorf("expected %q, got %q", pm.Markdown, string(content))
	}
}

func TestEmailSink_Validate(t *testing.T) {
	sink := &EmailSink{}
	if err := sink.Validate(); err == nil {
		t.Fatal("expected validation error for empty config")
	}

	sink = &EmailSink{
		Host:     "localhost",
		Port:     "25",
		Username: "user",
		Password: "password",
		To:       "to@example.com",
		From:     "from@example.com",
	}

	if err := sink.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
