package inmemory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"repo.nusatek.id/sugeng/walstreamer/broker"
	"repo.nusatek.id/sugeng/walstreamer/model"
)

// InMemoryBroker implements an enhanced in-memory message broker
type InMemoryBroker struct {
	messages     []interface{}
	mu           sync.RWMutex
	config       broker.BrokerConfig
	validators   []broker.MessageValidator
	transformers []broker.MessageTransformer
	metrics      broker.BrokerMetrics
	msgChan      chan *model.Message
	batchChan    chan []*model.Message
	done         chan struct{}
	wg           sync.WaitGroup
}

// NewInMemoryBroker creates a new instance of InMemoryBroker
func NewInMemoryBroker(config broker.BrokerConfig) *InMemoryBroker {
	b := &InMemoryBroker{
		messages:     make([]interface{}, 0),
		config:       config,
		validators:   make([]broker.MessageValidator, 0),
		transformers: make([]broker.MessageTransformer, 0),
		msgChan:      make(chan *model.Message, config.BufferSize),
		batchChan:    make(chan []*model.Message, config.BatchConfig.Size),
		done:         make(chan struct{}),
	}

	// Start batch workers
	b.startBatchWorkers()

	return b
}

// startBatchWorkers starts the batch processing workers
func (b *InMemoryBroker) startBatchWorkers() {
	b.wg.Add(b.config.BatchConfig.Workers)
	for i := 0; i < b.config.BatchConfig.Workers; i++ {
		go b.batchWorker()
	}
}

// batchWorker processes message batches
func (b *InMemoryBroker) batchWorker() {
	defer b.wg.Done()

	batch := make([]*model.Message, 0, b.config.BatchConfig.Size)
	timer := time.NewTimer(b.config.BatchConfig.FlushInterval)

	for {
		select {
		case msg := <-b.msgChan:
			batch = append(batch, msg)
			if len(batch) >= b.config.BatchConfig.Size {
				b.processBatch(batch)
				batch = make([]*model.Message, 0, b.config.BatchConfig.Size)
				timer.Reset(b.config.BatchConfig.FlushInterval)
			}
		case <-timer.C:
			if len(batch) > 0 {
				b.processBatch(batch)
				batch = make([]*model.Message, 0, b.config.BatchConfig.Size)
			}
			timer.Reset(b.config.BatchConfig.FlushInterval)
		case <-b.done:
			if len(batch) > 0 {
				b.processBatch(batch)
			}
			return
		}
	}
}

// processBatch processes a batch of messages
func (b *InMemoryBroker) processBatch(messages []*model.Message) {
	start := time.Now()
	formatted := make([]interface{}, 0, len(messages))

	for _, msg := range messages {
		if err := b.validateMessage(msg); err != nil {
			log.Error().Err(err).Msg("Message validation failed")
			b.metrics.MessagesFailed++
			continue
		}

		transformed, err := b.transformMessage(msg)
		if err != nil {
			log.Error().Err(err).Msg("Message transformation failed")
			b.metrics.MessagesFailed++
			continue
		}

		formatted = append(formatted, transformed.ToFormat(b.config.Format))
	}

	b.mu.Lock()
	b.messages = append(b.messages, formatted...)
	b.mu.Unlock()

	b.updateMetrics(len(messages), time.Since(start))
}

// Publish adds a message to the broker
func (b *InMemoryBroker) Publish(ctx context.Context, msg *model.Message) error {
	if msg == nil {
		return fmt.Errorf("cannot publish nil message")
	}

	select {
	case b.msgChan <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PublishBatch publishes a batch of messages
func (b *InMemoryBroker) PublishBatch(ctx context.Context, messages []*model.Message) error {
	if len(messages) == 0 {
		return nil
	}

	select {
	case b.batchChan <- messages:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// validateMessage applies all validators to a message
func (b *InMemoryBroker) validateMessage(msg *model.Message) error {
	for _, validator := range b.validators {
		if err := validator(msg); err != nil {
			return err
		}
	}
	return nil
}

// transformMessage applies all transformers to a message
func (b *InMemoryBroker) transformMessage(msg *model.Message) (*model.Message, error) {
	current := msg
	for _, transformer := range b.transformers {
		transformed, err := transformer(current)
		if err != nil {
			return nil, err
		}
		current = transformed
	}
	return current, nil
}

// updateMetrics updates broker metrics
func (b *InMemoryBroker) updateMetrics(count int, duration time.Duration) {
	b.metrics.MessagesPublished += int64(count)
	b.metrics.BatchesPublished++
	b.metrics.AverageLatency = (b.metrics.AverageLatency + duration) / 2
	b.metrics.LastPublishTime = time.Now()
	b.metrics.LastSuccessfulTime = time.Now()
	b.metrics.BufferSize = len(b.msgChan)
}

// AddValidator adds a message validator
func (b *InMemoryBroker) AddValidator(validator broker.MessageValidator) {
	b.validators = append(b.validators, validator)
}

// AddTransformer adds a message transformer
func (b *InMemoryBroker) AddTransformer(transformer broker.MessageTransformer) {
	b.transformers = append(b.transformers, transformer)
}

// Flush forces any buffered messages to be sent
func (b *InMemoryBroker) Flush(ctx context.Context) error {
	// Implementation for in-memory broker is a no-op
	return nil
}

// Health returns the current health status
func (b *InMemoryBroker) Health(ctx context.Context) error {
	// For in-memory broker, always healthy
	return nil
}

// Metrics returns the current metrics
func (b *InMemoryBroker) Metrics() broker.BrokerMetrics {
	return b.metrics
}

// GetMessages returns all stored messages
func (b *InMemoryBroker) GetMessages() []interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]interface{}, len(b.messages))
	copy(result, b.messages)
	return result
}

// Clear removes all stored messages
func (b *InMemoryBroker) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.messages = make([]interface{}, 0)
	b.metrics = broker.BrokerMetrics{}
}

// Close implements broker.Broker interface
func (b *InMemoryBroker) Close() error {
	close(b.done)
	b.wg.Wait()
	close(b.msgChan)
	close(b.batchChan)
	return nil
}
