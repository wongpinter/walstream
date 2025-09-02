package pipeline

import (
	"context"
	"fmt"

	"github.com/wongpinter/walstreamer/config"
	"github.com/wongpinter/walstreamer/logging"
	"github.com/wongpinter/walstreamer/pkg/gcloud/pubsub"
)

// pubSubTopicManager implements TopicManagerInterface
type pubSubTopicManager struct {
	pubManager pubsub.Manager
	config     *config.Config
	logger     *logging.Logger
}

// newPubSubTopicManager creates a new pubSubTopicManager instance
func newPubSubTopicManager(ctx context.Context, cfg *config.Config, logger *logging.Logger, creds string) (TopicManagerInterface, error) {
	pubMgr, err := pubsub.NewManager(ctx, cfg.Broker.PubSub.ProjectID, creds, cfg.Broker.PubSub.Location, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub manager: %w", err)
	}

	return &pubSubTopicManager{
		pubManager: pubMgr,
		config:     cfg,
		logger:     logger,
	}, nil
}

// CreateTopic implements TopicManagerInterface
func (tm *pubSubTopicManager) CreateTopic(ctx context.Context, topicID string, ordered bool) error {
	return tm.pubManager.CreateTopic(ctx, topicID, ordered)
}

// Close implements TopicManagerInterface
func (tm *pubSubTopicManager) Close() error {
	return tm.pubManager.Close()
}
