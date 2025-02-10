// Package pubsub provides a Google Cloud Pub/Sub implementation of the broker interface
package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/rs/zerolog"
	"google.golang.org/api/option"

	"repo.nusatek.id/sugeng/walstreamer/broker"
	"repo.nusatek.id/sugeng/walstreamer/model"
)

// Config holds configuration for Google Cloud Pub/Sub broker
type Config struct {
	// ProjectID is the Google Cloud project ID
	ProjectID string
	// TopicID is the Pub/Sub topic ID
	TopicID string
	// CredentialsFile is the path to the JSON credentials file
	CredentialsFile string
	// Logger is the zerolog logger instance
	Logger zerolog.Logger
}

// Broker implements the broker.Broker interface for Google Cloud Pub/Sub
type Broker struct {
	client       *pubsub.Client
	topic        *pubsub.Topic
	logger       zerolog.Logger
	metrics      broker.BrokerMetrics
	validators   []broker.MessageValidator
	transformers []broker.MessageTransformer
}

// NewBroker creates a new Google Cloud Pub/Sub broker
func NewBroker(cfg Config) (*Broker, error) {
	ctx := context.Background()

	// Create Pub/Sub client with credentials
	client, err := pubsub.NewClient(ctx, cfg.ProjectID, option.WithCredentialsFile(cfg.CredentialsFile))
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	// Get or create topic
	topic := client.Topic(cfg.TopicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to check if topic exists: %w", err)
	}

	if !exists {
		topic, err = client.CreateTopic(ctx, cfg.TopicID)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("failed to create topic: %w", err)
		}
		cfg.Logger.Info().Str("topic", cfg.TopicID).Msg("created new pubsub topic")
	}

	// Configure topic settings
	topic.PublishSettings = pubsub.PublishSettings{
		DelayThreshold: 10 * time.Millisecond,
		CountThreshold: 1, // Send immediately for real-time replication
	}

	return &Broker{
		client:       client,
		topic:        topic,
		logger:       cfg.Logger,
		metrics:      broker.BrokerMetrics{},
		validators:   make([]broker.MessageValidator, 0),
		transformers: make([]broker.MessageTransformer, 0),
	}, nil
}

// Publish publishes a message to Google Cloud Pub/Sub
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
	transformed := message
	for _, transformer := range b.transformers {
		var err error
		transformed, err = transformer(transformed)
		if err != nil {
			b.metrics.MessagesFailed++
			b.metrics.LastError = err
			b.metrics.LastErrorTime = time.Now()
			return fmt.Errorf("message transformation failed: %w", err)
		}
	}

	// Convert message to JSON
	data, err := json.Marshal(transformed)
	if err != nil {
		b.metrics.MessagesFailed++
		b.metrics.LastError = err
		b.metrics.LastErrorTime = time.Now()
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Create Pub/Sub message with metadata
	msg := &pubsub.Message{
		Data: data,
		Attributes: map[string]string{
			"operation": message.Operation,
			"schema":    message.Schema,
			"table":     message.Table,
			"lsn":       fmt.Sprintf("%d", message.LSN),
		},
	}

	// Publish message
	result := b.topic.Publish(ctx, msg)
	id, err := result.Get(ctx)
	if err != nil {
		b.metrics.MessagesFailed++
		b.metrics.LastError = err
		b.metrics.LastErrorTime = time.Now()
		return fmt.Errorf("failed to publish message: %w", err)
	}

	b.logger.Debug().
		Str("message_id", id).
		Str("operation", message.Operation).
		Str("table", message.Table).
		Uint64("lsn", message.LSN).
		Msg("published message to pubsub")

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
	b.topic.Stop() // Stop accepting new messages
	return nil
}

// Health returns the current health status
func (b *Broker) Health(ctx context.Context) error {
	// Check if topic exists
	exists, err := b.topic.Exists(ctx)
	if err != nil {
		return fmt.Errorf("failed to check topic health: %w", err)
	}
	if !exists {
		return fmt.Errorf("topic does not exist")
	}
	return nil
}

// Metrics returns the current broker metrics
func (b *Broker) Metrics() broker.BrokerMetrics {
	return b.metrics
}

// Close closes the Pub/Sub client and topic
func (b *Broker) Close() error {
	b.topic.Stop()
	return b.client.Close()
}
