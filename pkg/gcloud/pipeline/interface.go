package pipeline

import (
	"context"
	"fmt"
	"time"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/pkg/gcloud/bigquery"
)

const (
	// DefaultRetentionDuration specifies the default retention period for subscriptions
	DefaultRetentionDuration = 24 * time.Hour
)

// SchemaManagerInterface handles BigQuery schema operations
type SchemaManagerInterface interface {
	CreateDataset(ctx context.Context, datasetID string) error
	CreateSchema(ctx context.Context, configName string, tables []bigquery.SchemaTable) error
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
		config.Replication.SchemaName,
		table)
}
