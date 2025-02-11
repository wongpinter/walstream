package pubsub

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// PubSubClient defines the interface for Pub/Sub client operations
type PubSubClient interface {
	Topic(id string) TopicHandler
	CreateTopicWithConfig(ctx context.Context, id string, cfg *pubsub.TopicConfig) (TopicHandler, error)
	CreateSubscription(ctx context.Context, id string, cfg pubsub.SubscriptionConfig) (*pubsub.Subscription, error)
	Close() error
}

// TopicHandler defines the interface for Pub/Sub topic operations
type TopicHandler interface {
	String() string
	Exists(ctx context.Context) (bool, error)
}

// ClientWrapper wraps the Google Cloud Pub/Sub client to implement PubSubClient
type ClientWrapper struct {
	*pubsub.Client
}

// NewClientWrapper creates a new wrapper for the Pub/Sub client
func NewClientWrapper(client *pubsub.Client) PubSubClient {
	return &ClientWrapper{Client: client}
}

// Topic returns a TopicHandler for the given topic ID
func (w *ClientWrapper) Topic(id string) TopicHandler {
	return &TopicWrapper{Topic: w.Client.Topic(id)}
}

// CreateTopicWithConfig creates a new topic with the given configuration
func (w *ClientWrapper) CreateTopicWithConfig(ctx context.Context, id string, cfg *pubsub.TopicConfig) (TopicHandler, error) {
	topic, err := w.Client.CreateTopicWithConfig(ctx, id, cfg)
	if err != nil {
		return nil, err
	}
	return &TopicWrapper{Topic: topic}, nil
}

// TopicWrapper wraps a Pub/Sub topic to implement TopicHandler
type TopicWrapper struct {
	*pubsub.Topic
}

// String returns the topic's full path
func (w *TopicWrapper) String() string {
	return w.Topic.String()
}

// Exists checks if the topic exists
func (w *TopicWrapper) Exists(ctx context.Context) (bool, error) {
	return w.Topic.Exists(ctx)
}
