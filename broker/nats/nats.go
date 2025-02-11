package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/broker"
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
	Logger *zerolog.Logger
	// Username is the username for NATS authentication
	Username string
	// Password is the password for NATS authentication
	Password string
}

// Broker implements the broker.Broker interface for NATS
type Broker struct {
	conn         *nats.Conn
	js           nats.JetStreamContext
	subject      string
	logger       *zerolog.Logger
	metrics      broker.BrokerMetrics
	validators   []broker.MessageValidator
	transformers []broker.MessageTransformer
}

// NewBroker creates a new NATS broker
func NewBroker(cfg Config) (*Broker, error) {
	// Connect to NATS
	opts := cfg.Options
	if cfg.Username != "" {
		opts = append(opts, nats.Name(cfg.Username))
		if cfg.Password != "" {
			opts = append(opts, nats.UserInfo(cfg.Username, cfg.Password))
		}
	}

	nc, err := nats.Connect(cfg.URL, opts...)
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
	})
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	return &Broker{
		conn:         nc,
		js:           js,
		subject:      cfg.Subject,
		logger:       cfg.Logger,
		metrics:      broker.BrokerMetrics{},
		validators:   make([]broker.MessageValidator, 0),
		transformers: make([]broker.MessageTransformer, 0),
	}, nil
}

// Publish publishes a message to NATS
func (b *Broker) Publish(ctx context.Context, message *model.Message) error {
	// Validate message
	for _, validator := range b.validators {
		if err := validator(message); err != nil {
			b.metrics.MessagesFailed++
			b.metrics.LastError = err
			b.metrics.LastErrorTime = time.Now()
			return fmt.Errorf("message validation failed: %w", err)
		}
	}

	// Transform message
	transformed := interface{}(message)
	for _, transformer := range b.transformers {
		var err error
		transformed, err = transformer(message)
		if err != nil {
			b.metrics.MessagesFailed++
			b.metrics.LastError = err
			b.metrics.LastErrorTime = time.Now()
			return fmt.Errorf("message transformation failed: %w", err)
		}
	}

	b.logger.Debug().
		Str("operation", message.Operation).
		Str("table", message.Table).
		Uint64("lsn", message.LSN).
		Msg("publishing message to NATS")

	// Convert message to JSON
	data, err := json.Marshal(transformed)
	if err != nil {
		b.metrics.MessagesFailed++
		b.metrics.LastError = err
		b.metrics.LastErrorTime = time.Now()
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Publish with JetStream
	_, err = b.js.Publish(b.subject, data)
	if err != nil {
		b.metrics.MessagesFailed++
		b.metrics.LastError = err
		b.metrics.LastErrorTime = time.Now()
		return fmt.Errorf("failed to publish message: %w", err)
	}

	// Update metrics
	b.metrics.MessagesPublished++
	b.metrics.LastSuccessfulTime = time.Now()
	b.metrics.LastPublishTime = time.Now()

	return nil
}

// PublishBatch publishes a batch of messages
func (b *Broker) PublishBatch(ctx context.Context, messages []*model.Message) error {
	for _, msg := range messages {
		if err := b.Publish(ctx, msg); err != nil {
			return fmt.Errorf("failed to publish message in batch: %w", err)
		}
	}
	return nil
}

// AddValidator adds a message validator
func (b *Broker) AddValidator(validator broker.MessageValidator) {
	b.validators = append(b.validators, validator)
}

// AddTransformer adds a message transformer
func (b *Broker) AddTransformer(transformer broker.MessageTransformer) {
	b.transformers = append(b.transformers, transformer)
}

// Flush forces any buffered messages to be sent
func (b *Broker) Flush(ctx context.Context) error {
	return nil // NATS JetStream handles buffering
}

// Health returns the current health status
func (b *Broker) Health(ctx context.Context) error {
	if !b.conn.IsConnected() {
		return fmt.Errorf("not connected to NATS")
	}
	return nil
}

// Metrics returns the current broker metrics
func (b *Broker) Metrics() broker.BrokerMetrics {
	return b.metrics
}

// Close closes the NATS connection
func (b *Broker) Close() error {
	b.conn.Close()
	return nil
}
