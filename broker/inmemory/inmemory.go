package inmemory

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/rs/zerolog/log"

	"repo.nusatek.id/sugeng/walstreamer/model"
)

// InMemoryBroker implements a simple in-memory message broker
type InMemoryBroker struct {
	messages []interface{}
	mu       sync.RWMutex
	format   model.MessageFormat
}

// NewInMemoryBroker creates a new instance of InMemoryBroker
func NewInMemoryBroker(format model.MessageFormat) *InMemoryBroker {
	return &InMemoryBroker{
		messages: make([]interface{}, 0),
		format:   format,
	}
}

// Publish adds a message to the in-memory store
func (b *InMemoryBroker) Publish(ctx context.Context, msg *model.Message) error {
	if msg == nil {
		return fmt.Errorf("cannot publish nil message")
	}

	formatted := msg.ToFormat(b.format)
	data, err := json.Marshal(formatted)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.messages = append(b.messages, formatted)
	log.Debug().
		Str("operation", msg.Operation).
		Str("schema", msg.Schema).
		Str("table", msg.Table).
		Str("format", string(b.format)).
		RawJSON("message", data).
		Msg("Message published to in-memory broker")

	return nil
}

// GetMessages returns all stored messages
func (b *InMemoryBroker) GetMessages() []interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	messages := make([]interface{}, len(b.messages))
	copy(messages, b.messages)
	return messages
}

// Clear removes all stored messages
func (b *InMemoryBroker) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.messages = make([]interface{}, 0)
}

// Close implements broker.Broker interface
func (b *InMemoryBroker) Close() error {
	b.Clear()
	return nil
}
