package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
)

// DevHandler is a custom slog.Handler that formats logs into the visual bracket style:
// [LEVEL] [COMPONENT] [EVENT] | msg="..."
type DevHandler struct {
	opts slog.HandlerOptions
	out        io.Writer
	mu         *sync.Mutex
	instanceID string
}

func NewDevHandler(out io.Writer, opts *slog.HandlerOptions, instanceID string) *DevHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	return &DevHandler{
		opts:       *opts,
		out:        out,
		mu:         &sync.Mutex{},
		instanceID: instanceID,
	}
}

func (h *DevHandler) Enabled(ctx context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

func (h *DevHandler) Handle(ctx context.Context, r slog.Record) error {
	var component, event string
	var attrs []string

	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "component" {
			component = a.Value.String()
		} else if a.Key == "event" {
			event = a.Value.String()
		} else {
			attrs = append(attrs, fmt.Sprintf("%s=%q", a.Key, a.Value.String()))
		}
		return true
	})

	timeStr := r.Time.Format("2006-01-02 15:04:05.000")
	levelStr := fmt.Sprintf("[%-5s]", r.Level.String())

	prefix := fmt.Sprintf("%s %s", timeStr, levelStr)
	if h.instanceID != "" {
		prefix += fmt.Sprintf(" [%s]", h.instanceID)
	}
	
	if component != "" {
		prefix += fmt.Sprintf(" [%-15s]", strings.ToUpper(component))
	}
	if event != "" {
		prefix += fmt.Sprintf(" [%-15s]", strings.ToUpper(event))
	}

	msg := fmt.Sprintf("msg=%q", r.Message)
	if len(attrs) > 0 {
		msg += " " + strings.Join(attrs, " ")
	}

	outStr := fmt.Sprintf("%s | %s\n", prefix, msg)

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.out.Write([]byte(outStr))

	return err
}

func (h *DevHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h // Simplified for now, dev handler doesn't strictly need persistent attrs in this basic implementation
}

func (h *DevHandler) WithGroup(name string) slog.Handler {
	return h
}

// Setup configures the global logger based on environment.
func Setup(debug bool) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	opts := &slog.HandlerOptions{Level: level}
	
	format := os.Getenv("LOG_FORMAT")
	if format == "" {
		format = "text"
	}

	instanceID := os.Getenv("AEGIS_CP_ID")
	if instanceID == "" {
		instanceID = os.Getenv("AEGIS_WORKER_ID")
	}
	if instanceID == "" {
		instanceID, _ = os.Hostname()
	}

	var handler slog.Handler
	if format == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = NewDevHandler(os.Stdout, opts, instanceID)
	}

	logger := slog.New(handler)
	if instanceID != "" {
		logger = logger.With("instance_id", instanceID)
	}
	slog.SetDefault(logger)
	return logger
}
