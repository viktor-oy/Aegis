package kafka

import (
	"context"
	"encoding/json"
	"net"

	"time"

	"github.com/aegis/aegis/services/control-plane/internal/state"
	"github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
	brokers []string
	writer  *kafka.Writer
}

func NewKafkaPublisher(brokers []string) *KafkaPublisher {
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		KeepAlive: 30 * time.Second,
	}

	transport := &kafka.Transport{
		Dial: dialer.DialContext,
	}

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(brokers...),
		Transport:              transport,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: false,
		BatchTimeout:           10 * time.Millisecond,
		BatchSize:              1,
	}
	return &KafkaPublisher{
		brokers: brokers,
		writer:  writer,
	}
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}

func (p *KafkaPublisher) Publish(ctx context.Context, topic string, envelope state.EventEnvelope) error {
	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(envelope.WorkerID), // Partition by WorkerID
		Value: payloadBytes,
	})
}
