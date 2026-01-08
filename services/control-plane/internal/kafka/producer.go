package kafka

import (
	"context"
	"encoding/json"

	"github.com/aegis/aegis/services/control-plane/internal/state"
	"github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
	brokers []string
}

func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	return &KafkaPublisher{
		brokers: brokers,
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, topic string, envelope state.EventEnvelope) error {
	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(p.brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne, // Configurable if needed
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	return writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(envelope.WorkerID), // Partition by WorkerID
		Value: payloadBytes,
	})
}
