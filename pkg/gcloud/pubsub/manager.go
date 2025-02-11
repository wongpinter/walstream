package pubsub

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"

	"repo.nusatek.id/sugeng/walstreamer/logging"
)

// Manager handles Google Cloud Pub/Sub operations
type Manager interface {
	CreateTopic(ctx context.Context, topicID string, ordered bool) error
	CreateSubscription(ctx context.Context, topicID, subscriptionID string, cfg SubscriptionConfig) error
	TopicExists(ctx context.Context, topicID string) (bool, error)
	Close() error
}

// SubscriptionConfig holds configuration for a Pub/Sub subscription
type SubscriptionConfig struct {
	// Dead letter queue configuration
	DeadLetterTopic     string
	MaxDeliveryAttempts int // Must be between 5 and 100

	// Message retention configuration
	RetainAckedMessages bool
	RetentionDuration   time.Duration

	// Subscription configuration
	EnableMessageOrdering bool
	OrderingKey           string
	Filter                string
	ExpirationPolicy      time.Duration
	MinBackoff            time.Duration
	MaxBackoff            time.Duration

	// BigQuery configuration
	WithBigQuery *pubsub.BigQueryConfig
}

// PubSubManager implements Manager interface
type PubSubManager struct {
	client PubSubClient
	logger *logging.Logger
	region string
}

// NewManager creates a new Pub/Sub manager
func NewManager(ctx context.Context, projectID string, credentials string, region string, logger *logging.Logger) (Manager, error) {
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsFile(credentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	if region == "" {
		region = "us-central1" // Default region if not specified
	}

	return &PubSubManager{
		client: NewClientWrapper(client),
		logger: logger.WithComponent("pubsub"),
		region: region,
	}, nil
}

// CreateTopic creates a new Pub/Sub topic if it doesn't exist
func (m *PubSubManager) CreateTopic(ctx context.Context, topicID string, ordered bool) error {
	exists, err := m.TopicExists(ctx, topicID)
	if err != nil {
		return fmt.Errorf("failed to check topic existence: %w", err)
	}

	if exists {
		m.logger.Debug().Msgf("Topic %s already exists", topicID)
		return nil
	}

	topicConfig := &pubsub.TopicConfig{}
	if ordered {
		topicConfig.MessageStoragePolicy = pubsub.MessageStoragePolicy{
			AllowedPersistenceRegions: []string{m.region},
		}
	}

	topic, err := m.client.CreateTopicWithConfig(ctx, topicID, topicConfig)
	if err != nil {
		return fmt.Errorf("failed to create topic %s: %w", topicID, err)
	}

	m.logger.Info().
		Bool("ordered", ordered).
		Msgf("Created topic %s", topic.String())
	return nil
}

// CreateSubscription creates a new subscription to a topic
func (m *PubSubManager) CreateSubscription(ctx context.Context, topicID, subscriptionID string, cfg SubscriptionConfig) error {
	topic := m.client.Topic(topicID)

	// Check if topic exists
	exists, err := m.TopicExists(ctx, topicID)
	if err != nil {
		return fmt.Errorf("failed to check topic existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("topic %s does not exist", topicID)
	}

	// Validate MaxDeliveryAttempts
	if cfg.MaxDeliveryAttempts != 0 && (cfg.MaxDeliveryAttempts < 5 || cfg.MaxDeliveryAttempts > 100) {
		return fmt.Errorf("MaxDeliveryAttempts must be between 5 and 100, got %d", cfg.MaxDeliveryAttempts)
	}

	// Create subscription config
	subCfg := pubsub.SubscriptionConfig{
		Topic: topic.(*TopicWrapper).Topic,
	}

	if cfg.WithBigQuery != nil {
		subCfg.BigQueryConfig = *cfg.WithBigQuery
	}

	// Configure dead letter policy if specified
	if cfg.DeadLetterTopic != "" {
		deadLetterTopic := m.client.Topic(cfg.DeadLetterTopic)
		subCfg.DeadLetterPolicy = &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     deadLetterTopic.String(),
			MaxDeliveryAttempts: cfg.MaxDeliveryAttempts,
		}
	}

	// Configure message retention
	if cfg.RetainAckedMessages {
		subCfg.RetainAckedMessages = true
		if cfg.RetentionDuration > 0 {
			subCfg.RetentionDuration = cfg.RetentionDuration
		}
	}

	// Configure message ordering
	if cfg.EnableMessageOrdering {
		subCfg.EnableMessageOrdering = true
	}

	// Configure filter if specified
	if cfg.Filter != "" {
		subCfg.Filter = cfg.Filter
	}

	// Configure expiration policy
	if cfg.ExpirationPolicy > 0 {
		subCfg.ExpirationPolicy = cfg.ExpirationPolicy
	}

	// Configure retry policy
	if cfg.MinBackoff > 0 || cfg.MaxBackoff > 0 {
		subCfg.RetryPolicy = &pubsub.RetryPolicy{
			MinimumBackoff: cfg.MinBackoff,
			MaximumBackoff: cfg.MaxBackoff,
		}
	}

	// Create subscription
	sub, err := m.client.CreateSubscription(ctx, subscriptionID, subCfg)
	if err != nil {
		if strings.Contains(err.Error(), "AlreadyExists") {
			m.logger.Debug().Msgf("Subscription %s already exists", subscriptionID)
			return nil
		}
		m.logger.Error().Err(err).Any("subscription", subCfg).Msgf("Failed to create subscription %s", subscriptionID)

		return fmt.Errorf("failed to create subscription %s: %w", subscriptionID, err)
	}

	m.logger.Info().
		Str("topic", topicID).
		Bool("ordered", cfg.EnableMessageOrdering).
		Bool("hasFilter", cfg.Filter != "").
		Msgf("Created subscription %s", sub.ID())
	return nil
}

// TopicExists checks if a topic exists
func (m *PubSubManager) TopicExists(ctx context.Context, topicID string) (bool, error) {
	topic := m.client.Topic(topicID)
	exists, err := topic.Exists(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check topic existence: %w", err)
	}
	return exists, nil
}

// Close closes the Pub/Sub client
func (m *PubSubManager) Close() error {
	if err := m.client.Close(); err != nil {
		return fmt.Errorf("failed to close Pub/Sub client: %w", err)
	}
	return nil
}
