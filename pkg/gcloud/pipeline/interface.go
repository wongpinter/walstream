package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"repo.nusatek.id/sugeng/walstreamer/config"
)

const (
	// DefaultRetentionDuration specifies the default retention period for subscriptions
	DefaultRetentionDuration = 24 * time.Hour
)

// SchemaManagerInterface handles BigQuery schema operations
type SchemaManagerInterface interface {
	CreateDataset(ctx context.Context, datasetID string) error
	CreateSchema(ctx context.Context, configName string, tables []string) error
	Close() error
}

// TopicManagerInterface handles Pub/Sub topic operations
type TopicManagerInterface interface {
	CreateTopic(ctx context.Context, topicID string, ordered bool) error
	Close() error
}

// SubscriptionManagerInterface handles Pub/Sub subscription operations
type SubscriptionManagerInterface interface {
	CreateSubscription(ctx context.Context, configName string, tables []string) error
	Close() error
}

// utilities for pipeline operations

// FormatTopicID creates a consistent topic ID
func FormatTopicID(config *config.Config, table string) string {
	return fmt.Sprintf("%s.%s.%s",
		config.Broker.PubSub.TopicPrefix,
		config.Database.Schema,
		table)
}

func (*Pipeline) cleanTableNames(tables []string) {
	for i, table := range tables {
		parts := strings.Split(table, ".")
		if len(parts) == 2 {
			table = parts[1]
		}
		if len(parts) == 1 {
			table = parts[0]
		}
		tables[i] = table
	}
}
