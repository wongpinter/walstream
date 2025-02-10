package nats

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/model"
)

// Config holds configuration for NATS broker
type Config struct {
	// URL is the NATS server URL
	URL string
	// Subject is the NATS subject to publish messages to
	Subject string
	// Options are additional NATS connection options
	Options []nats.Option
	// Logger is the zerolog logger instance
	Logger zerolog.Logger
}

// Broker implements the broker.Broker interface for NATS
type Broker struct {
	conn    *nats.Conn
	js      nats.JetStreamContext
	subject string
	logger  zerolog.Logger
}

// NewBroker creates a new NATS broker
func NewBroker(cfg Config) (*Broker, error) {
	// Connect to NATS
	nc, err := nats.Connect(cfg.URL, cfg.Options...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	// Create JetStream context
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	// Create or update the stream
	streamName := "walstreamer"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{cfg.Subject},
		Storage:  nats.FileStorage,
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	return &Broker{
		conn:    nc,
		js:      js,
		subject: cfg.Subject,
		logger:  cfg.Logger,
	}, nil
}

// Publish publishes a message to NATS
func (b *Broker) Publish(ctx context.Context, message *model.Message) error {
	// Convert message to JSON
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	b.logger.Debug().
		Str("operation", message.Operation).
		Str("table", message.Table).
		Uint64("lsn", message.LSN).
		Msg("publishing message to NATS")

	// Publish with JetStream
	_, err = b.js.Publish(b.subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish message: %w", err)
	}

	return nil
}

// Close closes the NATS connection
func (b *Broker) Close() error {
	b.conn.Close()
	return nil
}
