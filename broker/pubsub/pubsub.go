// Package pubsub provides a Google Cloud Pub/Sub implementation of the broker interface
package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
	// TopicPrefix is the prefix for auto-generated topics
	TopicPrefix string
	// CredentialsFile is the path to the JSON credentials file
	CredentialsFile string
	// AutoCreateTopic determines whether to create topics automatically
	AutoCreateTopic bool
	// Logger is the zerolog logger instance
	Logger zerolog.Logger
}

// Broker implements the broker.Broker interface for Google Cloud Pub/Sub
type Broker struct {
	client       *pubsub.Client
	topics       map[string]*pubsub.Topic
	logger       zerolog.Logger
	metrics      broker.BrokerMetrics
	validators   []broker.MessageValidator
	transformers []broker.MessageTransformer
	config       Config
}

// NewBroker creates a new Google Cloud Pub/Sub broker
func NewBroker(cfg Config) (*Broker, error) {
	ctx := context.Background()

	// Create Pub/Sub client with credentials
	client, err := pubsub.NewClient(ctx, cfg.ProjectID, option.WithCredentialsFile(cfg.CredentialsFile))
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	return &Broker{
		client:       client,
		topics:       make(map[string]*pubsub.Topic),
		logger:       cfg.Logger,
		metrics:      broker.BrokerMetrics{},
		validators:   make([]broker.MessageValidator, 0),
		transformers: make([]broker.MessageTransformer, 0),
		config:       cfg,
	}, nil
}

// getTopicForTable gets or creates a Pub/Sub topic for the given table
func (b *Broker) getTopicForTable(ctx context.Context, table string) (*pubsub.Topic, error) {
	// Check if we already have the topic
	if topic, ok := b.topics[table]; ok {
		return topic, nil
	}

	// Generate topic ID from table name
	topicID := b.config.TopicPrefix + strings.Replace(table, ".", "-", -1)

	// Get or create topic
	topic := b.client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to check if topic exists: %w", err)
	}

	if !exists {
		if !b.config.AutoCreateTopic {
			return nil, fmt.Errorf("topic %s does not exist and auto-create is disabled", topicID)
		}
		topic, err = b.client.CreateTopic(ctx, topicID)
		if err != nil {
			return nil, fmt.Errorf("failed to create topic: %w", err)
		}
		b.logger.Info().Str("topic", topicID).Msg("created new pubsub topic")
	}

	// Configure topic settings
	topic.PublishSettings = pubsub.PublishSettings{
		DelayThreshold: 10 * time.Millisecond,
		CountThreshold: 1, // Send immediately for real-time replication
	}

	// Cache the topic
	b.topics[table] = topic
	return topic, nil
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

	// Get topic for this table
	topic, err := b.getTopicForTable(ctx, message.Table)
	if err != nil {
		b.metrics.MessagesFailed++
		b.metrics.LastError = err
		b.metrics.LastErrorTime = time.Now()
		return fmt.Errorf("failed to get topic for table %s: %w", message.Table, err)
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
	result := topic.Publish(ctx, msg)
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
		Str("topic", topic.ID()).
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
	// Stop all topics
	for _, topic := range b.topics {
		topic.Stop()
	}
	return nil
}

// Health returns the current health status
func (b *Broker) Health(ctx context.Context) error {
	// Check if all topics exist
	for table, topic := range b.topics {
		exists, err := topic.Exists(ctx)
		if err != nil {
			return fmt.Errorf("failed to check topic health for table %s: %w", table, err)
		}
		if !exists {
			return fmt.Errorf("topic for table %s does not exist", table)
		}
	}
	return nil
}

// Metrics returns the current broker metrics
func (b *Broker) Metrics() broker.BrokerMetrics {
	return b.metrics
}

// Close closes all topics and the Pub/Sub client
func (b *Broker) Close() error {
	// Stop all topics
	for _, topic := range b.topics {
		topic.Stop()
	}
	return b.client.Close()
}
