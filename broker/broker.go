package broker

import (
	"context"

	"repo.nusatek.id/sugeng/walstreamer/model"
)

// Broker defines the interface for message brokers
type Broker interface {
	// Publish publishes a message to the broker
	Publish(ctx context.Context, message *model.Message) error
	// Close closes the broker connection
	Close() error
}
