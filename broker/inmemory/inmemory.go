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
	messages     []*model.Message
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
		messages:     make([]*model.Message, 0),
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
	formatted := make([]*model.Message, 0, len(messages))

	for _, msg := range messages {
		if err := b.validateMessage(msg); err != nil {
			log.Error().Err(err).Msg("Message validation failed")
			b.mu.Lock()
			b.metrics.MessagesFailed++
			b.metrics.LastError = err
			b.metrics.LastErrorTime = time.Now()
			b.mu.Unlock()
			continue
		}

		transformed, err := b.transformMessage(msg)
		if err != nil {
			log.Error().Err(err).Msg("Message transformation failed")
			b.mu.Lock()
			b.metrics.MessagesFailed++
			b.metrics.LastError = err
			b.metrics.LastErrorTime = time.Now()
			b.mu.Unlock()
			continue
		}

		formatted = append(formatted, transformed)
	}

	b.mu.Lock()
	b.messages = append(b.messages, formatted...)
	b.metrics.MessagesPublished += int64(len(formatted))
	b.metrics.AverageLatency = time.Since(start)
	b.metrics.LastSuccessfulTime = time.Now()
	b.metrics.LastPublishTime = time.Now()
	b.metrics.BufferSize = len(b.messages) + len(b.msgChan)
	b.mu.Unlock()
}

// Publish adds a message to the broker
func (b *InMemoryBroker) Publish(ctx context.Context, msg *model.Message) error {
	select {
	case <-ctx.Done():
		b.mu.Lock()
		b.metrics.MessagesFailed++
		b.metrics.LastError = ctx.Err()
		b.metrics.LastErrorTime = time.Now()
		b.mu.Unlock()
		return ctx.Err()
	case b.msgChan <- msg:
		b.mu.Lock()
		b.metrics.LastSuccessfulTime = time.Now()
		b.metrics.LastPublishTime = time.Now()
		b.mu.Unlock()
		return nil
	default:
		b.mu.Lock()
		b.metrics.MessagesFailed++
		b.metrics.LastError = fmt.Errorf("message queue full (size: %d)", cap(b.msgChan))
		b.metrics.LastErrorTime = time.Now()
		b.mu.Unlock()
		return fmt.Errorf("message queue full (size: %d)", cap(b.msgChan))
	}
}

// PublishBatch publishes a batch of messages
func (b *InMemoryBroker) PublishBatch(ctx context.Context, messages []*model.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.batchChan <- messages:
		b.mu.Lock()
		b.metrics.BatchesPublished++
		b.mu.Unlock()
		return nil
	default:
		b.mu.Lock()
		b.metrics.BatchesFailed++
		b.mu.Unlock()
		return fmt.Errorf("batch queue full (size: %d)", cap(b.batchChan))
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
	var err error
	for _, transformer := range b.transformers {
		current, err = transformer(current)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
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
	// In-memory broker doesn't need explicit flushing
	return nil
}

// Health returns the current health status
func (b *InMemoryBroker) Health(ctx context.Context) error {
	// In-memory broker is always healthy
	return nil
}

// Metrics returns the current metrics
func (b *InMemoryBroker) Metrics() broker.BrokerMetrics {
	b.mu.RLock()
	defer b.mu.RUnlock()

	metrics := b.metrics
	metrics.BufferSize = len(b.messages) + len(b.msgChan)
	return metrics
}

// GetMessages returns all stored messages
func (b *InMemoryBroker) GetMessages() []*model.Message {
	b.mu.RLock()
	defer b.mu.RUnlock()
	messages := make([]*model.Message, len(b.messages))
	copy(messages, b.messages)
	return messages
}

// Clear removes all stored messages
func (b *InMemoryBroker) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages = make([]*model.Message, 0)
	b.metrics = broker.BrokerMetrics{}
}

// Close implements broker.Broker interface
func (b *InMemoryBroker) Close() error {
	close(b.done)
	b.wg.Wait()
	return nil
}
