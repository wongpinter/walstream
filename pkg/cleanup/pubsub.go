package cleanup

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	"github.com/wongpinter/walstreamer/config"
	"github.com/wongpinter/walstreamer/logging"
)

// Pub/Sub naming patterns
var (
	topicNameRegexp        = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-_.~+%]{2,254}$`)
	subscriptionNameRegexp = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9-_.~+%]{2,254}$`)
	subscriptionSuffixes   = []string{"_sub", "_sub.bq", "_deleted_topic"}
)

// pubSubCleaner implements PubSubCleanerInterface
type pubSubCleaner struct {
	client    *pubsub.Client
	logger    *logging.Logger
	config    *config.Config
	projectID string
}

// NewPubSubCleaner creates a new Pub/Sub cleaner
func NewPubSubCleaner(ctx context.Context, cfg *config.Config, credentials string, logger *logging.Logger) (PubSubCleanerInterface, error) {
	client, err := pubsub.NewClient(ctx, cfg.Broker.PubSub.ProjectID, option.WithCredentialsFile(credentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub client: %w", err)
	}

	return &pubSubCleaner{
		client:    client,
		logger:    logger.WithComponent("pubsub-cleaner"),
		config:    cfg,
		projectID: cfg.Broker.PubSub.ProjectID,
	}, nil
}

// isValidPubSubName checks if a name is valid for Pub/Sub resources
func isValidPubSubName(name string, isSubscription bool) bool {
	if name == "" {
		return false
	}
	if isSubscription {
		return subscriptionNameRegexp.MatchString(name)
	}
	return topicNameRegexp.MatchString(name)
}

// isDebeziumTopic checks if a topic matches the Debezium configuration pattern
func (pc *pubSubCleaner) isDebeziumTopic(topicID string) bool {
	prefix := pc.config.Broker.PubSub.TopicPrefix
	schema := pc.config.Replication.SchemaName
	pattern := fmt.Sprintf("%s.%s.", prefix, schema)
	return strings.HasPrefix(topicID, pattern)
}

// getTableFromTopicID extracts table name from a topic ID
func (pc *pubSubCleaner) getTableFromTopicID(topicID string) string {
	parts := strings.Split(topicID, ".")
	if len(parts) < 3 {
		return ""
	}
	return parts[2]
}

// isConfiguredTable checks if a table is in the configured include list
func (pc *pubSubCleaner) isConfiguredTable(tableName string) bool {
	for _, pattern := range pc.config.Replication.GetTableNames() {
		parts := strings.Split(pattern, ".")
		if len(parts) >= 2 {
			if parts[1] == tableName {
				return true
			}
		} else if parts[0] == tableName {
			return true
		}
	}
	return false
}

// isDebeziumSubscription checks if a subscription matches Debezium patterns
func (pc *pubSubCleaner) isDebeziumSubscription(subID string) bool {
	// Check if subscription has any of the valid suffixes
	for _, suffix := range subscriptionSuffixes {
		if strings.HasSuffix(subID, suffix) {
			// Remove suffix and check if remaining part is a Debezium topic ID
			baseTopicID := strings.TrimSuffix(subID, suffix)
			return pc.isDebeziumTopic(baseTopicID)
		}
	}
	return false
}

// cleanResourcePath removes the project and resource type prefix from a full resource path
func cleanResourcePath(path string) string {
	parts := strings.Split(path, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return path
}

// ListResources implements PubSubCleanerInterface
func (pc *pubSubCleaner) ListResources(ctx context.Context) ([]ResourceInfo, error) {
	var resources []ResourceInfo
	topicMap := make(map[string]ResourceInfo)

	// List topics first
	topicIt := pc.client.Topics(ctx)
	for {
		topic, err := topicIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error listing topics: %w", err)
		}

		topicID := cleanResourcePath(topic.String())

		// Validate topic name
		if !isValidPubSubName(topicID, false) {
			pc.logger.Debug().Str("topic", topicID).Msg("Skipping invalid topic name")
			continue
		}

		// Only include topics that match Debezium pattern and configured tables
		if !pc.isDebeziumTopic(topicID) {
			continue
		}

		tableName := pc.getTableFromTopicID(topicID)
		if !pc.isConfiguredTable(tableName) {
			continue
		}

		topicInfo := ResourceInfo{
			Type:    "topic",
			Name:    topicID,
			Project: pc.projectID,
		}
		topicMap[topicID] = topicInfo
	}

	// List subscriptions
	subIt := pc.client.Subscriptions(ctx)
	for {
		sub, err := subIt.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error listing subscriptions: %w", err)
		}

		subID := cleanResourcePath(sub.String())

		// Validate subscription name
		if !isValidPubSubName(subID, true) {
			pc.logger.Debug().Str("subscription", subID).Msg("Skipping invalid subscription name")
			continue
		}

		// Check if it's a Debezium subscription
		if !pc.isDebeziumSubscription(subID) {
			continue
		}

		// Get subscription config to find associated topic
		cfg, err := sub.Config(ctx)
		if err != nil {
			pc.logger.Warn().
				Str("subscription", subID).
				Err(err).
				Msg("Failed to get subscription config, skipping")
			continue
		}

		topicID := cleanResourcePath(cfg.Topic.String())

		subInfo := ResourceInfo{
			Type:      "subscription",
			Name:      subID,
			Project:   pc.projectID,
			DependsOn: []string{topicID},
		}

		// Add to resources even if topic doesn't exist (for _deleted_topic subscriptions)
		resources = append(resources, subInfo)

		// Update topic's RequiredFor list if topic exists
		if topic, ok := topicMap[topicID]; ok {
			topic.RequiredFor = append(topic.RequiredFor, subID)
			topicMap[topicID] = topic
		}
	}

	// Add topics to resources
	for _, topic := range topicMap {
		resources = append(resources, topic)
	}

	return resources, nil
}

// DeleteTopic implements PubSubCleanerInterface
func (pc *pubSubCleaner) DeleteTopic(ctx context.Context, name string) error {
	if !isValidPubSubName(name, false) {
		return fmt.Errorf("invalid topic name format: %s", name)
	}

	// Verify it's a Debezium topic
	if !pc.isDebeziumTopic(name) {
		return fmt.Errorf("topic %s is not a Debezium topic", name)
	}

	topic := pc.client.Topic(name)

	exists, err := topic.Exists(ctx)
	if err != nil {
		return fmt.Errorf("error checking topic %s: %w", name, err)
	}
	if !exists {
		pc.logger.Debug().Str("topic", name).Msg("Topic does not exist")
		return nil
	}

	// Get topic configuration for logging
	cfg, err := topic.Config(ctx)
	if err != nil {
		pc.logger.Warn().Str("topic", name).Err(err).Msg("Failed to get topic config")
	} else {
		pc.logger.Info().
			Str("topic", name).
			Str("messageStoragePolicy", strings.Join(cfg.MessageStoragePolicy.AllowedPersistenceRegions, ",")).
			Bool("schemaSettings", cfg.SchemaSettings != nil).
			Msg("Deleting topic")
	}

	if err := topic.Delete(ctx); err != nil {
		return fmt.Errorf("error deleting topic %s: %w", name, err)
	}

	return nil
}

// DeleteSubscription implements PubSubCleanerInterface
func (pc *pubSubCleaner) DeleteSubscription(ctx context.Context, name string) error {
	if !isValidPubSubName(name, true) {
		return fmt.Errorf("invalid subscription name format: %s", name)
	}

	// Verify it's a Debezium subscription
	if !pc.isDebeziumSubscription(name) {
		return fmt.Errorf("subscription %s is not a Debezium subscription", name)
	}

	sub := pc.client.Subscription(name)

	exists, err := sub.Exists(ctx)
	if err != nil {
		return fmt.Errorf("error checking subscription %s: %w", name, err)
	}
	if !exists {
		pc.logger.Debug().Str("subscription", name).Msg("Subscription does not exist")
		return nil
	}

	// Get subscription configuration for logging
	cfg, err := sub.Config(ctx)
	if err != nil {
		pc.logger.Warn().Str("subscription", name).Err(err).Msg("Failed to get subscription config")
	} else {
		pc.logger.Info().
			Str("subscription", name).
			Str("topic", cleanResourcePath(cfg.Topic.String())).
			Bool("orderingEnabled", cfg.EnableMessageOrdering).
			Dur("retentionDuration", cfg.RetentionDuration).
			Msg("Deleting subscription")
	}

	if err := sub.Delete(ctx); err != nil {
		return fmt.Errorf("error deleting subscription %s: %w", name, err)
	}

	return nil
}

// VerifyResourceExists implements PubSubCleanerInterface
func (pc *pubSubCleaner) VerifyResourceExists(ctx context.Context, resType, name string) (bool, error) {
	switch resType {
	case "topic":
		if !isValidPubSubName(name, false) {
			return false, fmt.Errorf("invalid topic name format: %s", name)
		}
		// Verify it's a Debezium topic
		if !pc.isDebeziumTopic(name) {
			return false, fmt.Errorf("topic %s is not a Debezium topic", name)
		}

		topic := pc.client.Topic(name)
		exists, err := topic.Exists(ctx)
		if err != nil {
			return false, fmt.Errorf("error checking topic %s: %w", name, err)
		}
		return exists, nil

	case "subscription":
		if !isValidPubSubName(name, true) {
			return false, fmt.Errorf("invalid subscription name format: %s", name)
		}
		// Verify it's a Debezium subscription
		if !pc.isDebeziumSubscription(name) {
			return false, fmt.Errorf("subscription %s is not a Debezium subscription", name)
		}

		sub := pc.client.Subscription(name)
		exists, err := sub.Exists(ctx)
		if err != nil {
			return false, fmt.Errorf("error checking subscription %s: %w", name, err)
		}
		return exists, nil

	default:
		return false, fmt.Errorf("unsupported resource type: %s", resType)
	}
}

// Close implements PubSubCleanerInterface
func (pc *pubSubCleaner) Close() error {
	if err := pc.client.Close(); err != nil {
		return fmt.Errorf("error closing Pub/Sub client: %w", err)
	}
	return nil
}
