package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/aegis/aegis/services/sink/internal/delivery"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafka.Writer
	logger *slog.Logger
}

func NewPublisher(brokers []string, logger *slog.Logger) *Publisher {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		AllowAutoTopicCreation: true,
	}
	return &Publisher{writer: w, logger: logger}
}

func (p *Publisher) PublishStatus(ctx context.Context, topic string, result delivery.Result) error {
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
		p.logger.Warn("Failed to publish status, retrying...", "topic", topic, "error", err, "attempt", i+1)
		time.Sleep(500 * time.Millisecond)
	}

	if err != nil {
		p.logger.Error("Failed to publish status after retries", "topic", topic, "incident_id", result.IncidentID, "error", err)
		return err
	}
	p.logger.Debug("Published delivery status", "topic", topic, "incident_id", result.IncidentID, "status", result.Status)
	return nil
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}
