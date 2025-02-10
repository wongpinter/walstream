package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"

	"repo.nusatek.id/sugeng/walstreamer/broker"
	"repo.nusatek.id/sugeng/walstreamer/model"
)

// RabbitMQBroker implements the broker.Broker interface for RabbitMQ
type RabbitMQBroker struct {
	config       broker.BrokerConfig
	rabbitCfg    Config
	conn         *amqp.Connection
	ch           *amqp.Channel
	validators   []broker.MessageValidator
	transformers []broker.MessageTransformer
	metrics      broker.BrokerMetrics
	msgChan      chan *model.Message
	batchChan    chan []*model.Message
	done         chan struct{}
	wg           sync.WaitGroup
}

// NewRabbitMQBroker creates a new RabbitMQ broker instance
func NewRabbitMQBroker(brokerConfig broker.BrokerConfig, rabbitConfig Config) (*RabbitMQBroker, error) {
	if err := rabbitConfig.Validate(); err != nil {
		return nil, fmt.Errorf("invalid RabbitMQ configuration: %w", err)
	}

	b := &RabbitMQBroker{
		config:       brokerConfig,
		rabbitCfg:    rabbitConfig,
		validators:   make([]broker.MessageValidator, 0),
		transformers: make([]broker.MessageTransformer, 0),
		msgChan:      make(chan *model.Message, brokerConfig.BufferSize),
		batchChan:    make(chan []*model.Message, brokerConfig.BatchConfig.Size),
		done:         make(chan struct{}),
	}

	if err := b.connect(); err != nil {
		return nil, err
	}

	b.startBatchWorkers()
	go b.reconnectLoop()

	return b, nil
}

// connect establishes connection to RabbitMQ and sets up exchange and queue
func (b *RabbitMQBroker) connect() error {
	conn, err := amqp.Dial(b.rabbitCfg.URL())
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}

	// Set QoS
	if err := ch.Qos(
		b.rabbitCfg.PrefetchCount,
		b.rabbitCfg.PrefetchSize,
		b.rabbitCfg.PrefetchGlobal,
	); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Declare exchange
	if err := ch.ExchangeDeclare(
		b.rabbitCfg.ExchangeName,
		b.rabbitCfg.ExchangeType,
		b.rabbitCfg.Durable,
		b.rabbitCfg.AutoDelete,
		false, // internal
		false, // no-wait
		nil,   // arguments
	); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	// Declare queue
	_, err = ch.QueueDeclare(
		b.rabbitCfg.QueueName,
		b.rabbitCfg.QueueDurable,
		b.rabbitCfg.QueueAutoDelete,
		b.rabbitCfg.Exclusive,
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	// Bind queue to exchange
	err = ch.QueueBind(
		b.rabbitCfg.QueueName,
		b.rabbitCfg.RoutingKey,
		b.rabbitCfg.ExchangeName,
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	b.conn = conn
	b.ch = ch

	return nil
}

// reconnectLoop handles reconnection to RabbitMQ
func (b *RabbitMQBroker) reconnectLoop() {
	for {
		select {
		case <-b.done:
			return
		case <-b.conn.NotifyClose(make(chan *amqp.Error)):
			log.Error().Msg("Lost connection to RabbitMQ, attempting to reconnect...")

			for {
				err := b.connect()
				if err == nil {
					log.Info().Msg("Successfully reconnected to RabbitMQ")
					break
				}

				log.Error().Err(err).Msg("Failed to reconnect to RabbitMQ")
				select {
				case <-b.done:
					return
				case <-time.After(b.rabbitCfg.ReconnectDelay):
				}
			}
		}
	}
}

// startBatchWorkers starts the batch processing workers
func (b *RabbitMQBroker) startBatchWorkers() {
	b.wg.Add(b.config.BatchConfig.Workers)
	for i := 0; i < b.config.BatchConfig.Workers; i++ {
		go b.batchWorker()
	}
}

// batchWorker processes message batches
func (b *RabbitMQBroker) batchWorker() {
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
func (b *RabbitMQBroker) processBatch(messages []*model.Message) {
	start := time.Now()
	published := 0

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

		data, err := json.Marshal(transformed.ToFormat(b.config.Format))
		if err != nil {
			log.Error().Err(err).Msg("Failed to marshal message")
			b.metrics.MessagesFailed++
			continue
		}

		err = b.ch.PublishWithContext(
			context.Background(),
			b.rabbitCfg.ExchangeName,
			b.rabbitCfg.RoutingKey,
			b.rabbitCfg.Mandatory,
			b.rabbitCfg.Immediate,
			amqp.Publishing{
				ContentType:  b.rabbitCfg.ContentType,
				Body:         data,
				DeliveryMode: amqp.Persistent,
				Timestamp:    time.Now(),
			},
		)
		if err != nil {
			log.Error().Err(err).Msg("Failed to publish message")
			b.metrics.MessagesFailed++
			continue
		}

		published++
	}

	b.updateMetrics(published, time.Since(start))
}

// validateMessage applies all validators to a message
func (b *RabbitMQBroker) validateMessage(msg *model.Message) error {
	for _, validator := range b.validators {
		if err := validator(msg); err != nil {
			return err
		}
	}
	return nil
}

// transformMessage applies all transformers to a message
func (b *RabbitMQBroker) transformMessage(msg *model.Message) (*model.Message, error) {
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
func (b *RabbitMQBroker) updateMetrics(count int, duration time.Duration) {
	b.metrics.MessagesPublished += int64(count)
	b.metrics.BatchesPublished++
	b.metrics.AverageLatency = (b.metrics.AverageLatency + duration) / 2
	b.metrics.LastPublishTime = time.Now()
	b.metrics.LastSuccessfulTime = time.Now()
	b.metrics.BufferSize = len(b.msgChan)
}

// Publish publishes a single message
func (b *RabbitMQBroker) Publish(ctx context.Context, msg *model.Message) error {
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
func (b *RabbitMQBroker) PublishBatch(ctx context.Context, messages []*model.Message) error {
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

// AddValidator adds a message validator
func (b *RabbitMQBroker) AddValidator(validator broker.MessageValidator) {
	b.validators = append(b.validators, validator)
}

// AddTransformer adds a message transformer
func (b *RabbitMQBroker) AddTransformer(transformer broker.MessageTransformer) {
	b.transformers = append(b.transformers, transformer)
}

// Flush forces any buffered messages to be sent
func (b *RabbitMQBroker) Flush(ctx context.Context) error {
	return b.ch.Close()
}

// Health checks the broker's health
func (b *RabbitMQBroker) Health(ctx context.Context) error {
	if b.conn == nil || b.conn.IsClosed() {
		return fmt.Errorf("connection is closed")
	}
	if b.ch == nil || b.ch.IsClosed() {
		return fmt.Errorf("channel is closed")
	}
	return nil
}

// Metrics returns the current metrics
func (b *RabbitMQBroker) Metrics() broker.BrokerMetrics {
	return b.metrics
}

// Close closes the broker connection
func (b *RabbitMQBroker) Close() error {
	close(b.done)
	b.wg.Wait()

	if b.ch != nil {
		if err := b.ch.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close channel")
		}
	}
	if b.conn != nil {
		if err := b.conn.Close(); err != nil {
			log.Error().Err(err).Msg("Failed to close connection")
		}
	}

	close(b.msgChan)
	close(b.batchChan)
	return nil
}
