package message

import (
	"time"

	"github.com/dakasa-co/yggdrasil-integration-kubernetes/internal/adapter"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

// RegisterAllConsumers declares the adapter queues and starts the consumers.
func RegisterAllConsumers(conn *amqp.Connection, logger *zap.Logger) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	for _, cfg := range consumers(conn, logger) {
		if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, nil); err != nil {
			_ = ch.Close()
			return err
		}
		if err := consumeQueue(ch, cfg); err != nil {
			_ = ch.Close()
			return err
		}
	}

	return nil
}

// Queues returns the queue names implemented by this adapter.
func Queues() []string {
	return []string{
		adapter.QueueDescribe,
		adapter.QueueExecute,
	}
}

func consumers(conn *amqp.Connection, logger *zap.Logger) []ConsumerConfig {
	return []ConsumerConfig{
		{
			Queue:   adapter.QueueDescribe,
			Timeout: 10 * time.Second,
			QoS:     10,
			Handler: describeHandler(conn, logger),
		},
		{
			Queue:   adapter.QueueExecute,
			Timeout: 30 * time.Second,
			QoS:     5,
			Handler: executeHandler(conn, logger),
		},
	}
}
