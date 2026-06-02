package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/aegis/aegis/services/sink/internal/types"

	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(brokers []string) *Publisher {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		KeepAlive: 30 * time.Second,
	}

	transport := &kafka.Transport{
		Dial: dialer.DialContext,
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Transport:              transport,
		AllowAutoTopicCreation: false,
	}
	return &Publisher{writer: w}
}

func (p *Publisher) PublishStatus(ctx context.Context, topic string, result types.Result) error {
	b, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %w", err)
	}

	for i := 0; i < 10; i++ {
		err = p.writer.WriteMessages(ctx, kafka.Message{
			Topic: topic,
			Key:   []byte(result.IncidentID),
			Value: b,
		})
		if err == nil {
			break
		}
		slog.Warn("Failed to publish status, retrying...", "component", "KAFKA", "event", "PUBLISH_RETRY", "topic", topic, "error", err, "attempt", i+1)
		time.Sleep(500 * time.Millisecond)
	}

	if err != nil {
		slog.Error("Failed to publish status after retries", "component", "KAFKA", "event", "PUBLISH_ERR", "topic", topic, "incident_id", result.IncidentID, "error", err)
		return err
	}
	slog.Debug("Published delivery status", "component", "KAFKA", "event", "PUBLISH_SUCCESS", "topic", topic, "incident_id", result.IncidentID, "status", result.Status)
	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
