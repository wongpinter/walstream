package inmemory

import (
	"context"
	"sync"

	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/model"
)

// InMemoryBroker is an in-memory implementation of the Broker interface
// It's useful for testing and development
type InMemoryBroker struct {
	messages []*model.Message
	mu       sync.RWMutex
	logger   *zerolog.Logger
}

// NewInMemoryBroker creates a new InMemoryBroker
func NewInMemoryBroker(l *zerolog.Logger) *InMemoryBroker {
	return &InMemoryBroker{
		messages: make([]*model.Message, 0),
		logger:   l,
	}
}

// Publish stores the message in memory
func (b *InMemoryBroker) Publish(_ context.Context, message *model.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages = append(b.messages, message)
	return nil
}

// Close is a no-op for InMemoryBroker
func (b *InMemoryBroker) Close() error {
	return nil
}

// GetMessages returns all stored messages
// This is useful for testing
func (b *InMemoryBroker) GetMessages() []*model.Message {
	b.mu.RLock()
	defer b.mu.RUnlock()
	messages := make([]*model.Message, len(b.messages))
	copy(messages, b.messages)
	b.logger.Info().Any("messages", messages).Msg("messages")
	return messages
}
