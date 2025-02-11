package pipeline

import (
	"context"
	"fmt"
	"strings"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
	"repo.nusatek.id/sugeng/walstreamer/pkg/gcloud/pubsub"
)

// pubSubSubscriptionManager implements SubscriptionManagerInterface
type pubSubSubscriptionManager struct {
	pubManager pubsub.Manager
	config     *config.Config
	logger     *logging.Logger
	creds      string
}

// newPubSubSubscriptionManager creates a new pubSubSubscriptionManager instance
func newPubSubSubscriptionManager(ctx context.Context, cfg *config.Config, logger *logging.Logger, creds string) (SubscriptionManagerInterface, error) {
	pubMgr, err := pubsub.NewManager(ctx, cfg.Broker.PubSub.ProjectID, creds, cfg.Broker.PubSub.Location, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub manager: %w", err)
	}

	return &pubSubSubscriptionManager{
		pubManager: pubMgr,
		config:     cfg,
		logger:     logger,
		creds:      creds,
	}, nil
}

// CreateSubscription implements SubscriptionManagerInterface
func (sm *pubSubSubscriptionManager) CreateSubscription(ctx context.Context, configName string, tables []string) error {
	for _, table := range tables {
		topicID := FormatTopicID(sm.config, table)
		subID := fmt.Sprintf("%s_sub", topicID)
		bqSubID := fmt.Sprintf("%s_sub.bq", topicID)

		sm.logger.Info().
			Str("topic", topicID).
			Str("subscription", subID).
			Msg("Creating subscriptions")

		// Create pull subscription
		subCfg := pubsub.SubscriptionConfig{
			RetainAckedMessages: true,
			RetentionDuration:   DefaultRetentionDuration,
		}
		if err := sm.pubManager.CreateSubscription(ctx, topicID, subID, subCfg); err != nil {
			// Check if error is "already exists"
			if strings.Contains(err.Error(), "AlreadyExists") {
				sm.logger.Info().
					Str("subscription", subID).
					Msg("Subscription already exists")
			} else {
				return fmt.Errorf("failed to create subscription %s: %w", subID, err)
			}
		}

		// Create BigQuery subscription
		bqSub, err := pubsub.NewBQSubscriber(
			ctx,
			sm.config.Broker.PubSub.ProjectID,
			sm.logger,
			pubsub.WithCredentialsFile(ctx, sm.creds),
		)
		if err != nil {
			return fmt.Errorf("failed to create BigQuery subscriber: %w", err)
		}

		bqSubCfg := pubsub.BQSubscriptionConfig{
			TopicID:        topicID,
			SubscriptionID: bqSubID,
			TableRef: fmt.Sprintf("%s.%s.%s",
				sm.config.Broker.PubSub.ProjectID,
				sm.config.Broker.Topic,
				table),
			RetentionDuration: DefaultRetentionDuration,
			WriteMetadata:     sm.config.Broker.WriteMetadata,
		}

		if err := bqSub.CreateSubscription(ctx, bqSubCfg); err != nil {
			// Check if error is "already exists"
			if strings.Contains(err.Error(), "ALREADY_EXISTS") {
				sm.logger.Info().
					Str("subscription", bqSubID).
					Msg("BigQuery subscription already exists")
			} else {
				return fmt.Errorf("failed to create BigQuery subscription %s: %w", bqSubID, err)
			}
		}
	}

	return nil
}

// Close implements SubscriptionManagerInterface
func (sm *pubSubSubscriptionManager) Close() error {
	return sm.pubManager.Close()
}
