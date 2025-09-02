package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"

	"github.com/wongpinter/walstreamer/broker"
	"github.com/wongpinter/walstreamer/model"
)

// RabbitMQBroker implements the broker.Broker interface for RabbitMQ
type RabbitMQBroker struct {
	config Config
	pool   *connectionPool
	stats  *brokerStats

	// Message handling
	messages  chan *model.Message
	batchChan chan []*model.Message
	done      chan struct{}
	wg        sync.WaitGroup

	// Resource management
	memUsage    atomic.Int64
	workerCount atomic.Int32
	isShutdown  atomic.Bool
}

// connectionPool manages a pool of RabbitMQ connections
type connectionPool struct {
	mu          sync.RWMutex
	connections []*amqp.Connection
	channels    map[*amqp.Connection][]*amqp.Channel
	config      Config
}

// brokerStats holds runtime statistics
type brokerStats struct {
	messagesPublished atomic.Int64
	publishErrors     atomic.Int64
	reconnectCount    atomic.Int64
	lastError         atomic.Value
	lastPublishTime   atomic.Int64
}

// NewRabbitMQBroker creates a new RabbitMQ broker instance
func NewRabbitMQBroker(cfg Config) (*RabbitMQBroker, error) {
	pool, err := newConnectionPool(cfg)
	if err != nil {
		return nil, errors.New("failed to create connection pool: " + err.Error())
	}

	b := &RabbitMQBroker{
		config:    cfg,
		pool:      pool,
		stats:     &brokerStats{},
		messages:  make(chan *model.Message, cfg.MaxQueueSize),
		batchChan: make(chan []*model.Message),
		done:      make(chan struct{}),
	}

	// Start resource monitoring
	go b.monitorResources()

	// Start worker pool
	b.startWorkers()

	return b, nil
}

// Publish publishes a message to RabbitMQ
func (b *RabbitMQBroker) Publish(ctx context.Context, msg *model.Message) error {
	if b.isShutdown.Load() {
		return broker.ErrBrokerClosed
	}

	// Check memory usage
	if b.memUsage.Load() > int64(b.config.MaxMemoryMB)*1024*1024 {
		return errors.New("memory limit exceeded: " + fmt.Sprintf("%d MB", b.config.MaxMemoryMB))
	}

	select {
	case b.messages <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.New("message queue full (size: " + fmt.Sprintf("%d", b.config.MaxQueueSize) + ")")
	}
}

// Close gracefully shuts down the broker
func (b *RabbitMQBroker) Close() error {
	if !b.isShutdown.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	close(b.done)

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		b.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info().Msg("All workers gracefully stopped")
	case <-time.After(b.config.ShutdownTimeout):
		log.Warn().Msg("Shutdown timeout reached, some workers may still be running")
	}

	return b.pool.close()
}

// startWorkers starts the worker pool
func (b *RabbitMQBroker) startWorkers() {
	for i := 0; i < b.config.MaxWorkers; i++ {
		b.wg.Add(1)
		b.workerCount.Add(1)
		go b.worker()
	}
}

// worker processes messages from the queue
func (b *RabbitMQBroker) worker() {
	defer b.wg.Done()
	defer b.workerCount.Add(-1)

	for {
		select {
		case <-b.done:
			return
		case msg := <-b.messages:
			if err := b.publishMessage(msg); err != nil {
				b.stats.publishErrors.Add(1)
				b.stats.lastError.Store(err.Error())
				log.Error().Err(err).Msg("Failed to publish message")
			} else {
				b.stats.messagesPublished.Add(1)
				b.stats.lastPublishTime.Store(time.Now().UnixNano())
			}
		}
	}
}

// publishMessage publishes a single message to RabbitMQ
func (b *RabbitMQBroker) publishMessage(msg *model.Message) error {
	ch, err := b.pool.getChannel()
	if err != nil {
		return errors.New("failed to get channel: " + err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), b.config.PublishTimeout)
	defer cancel()

	body, err := msg.MarshalJSON()
	if err != nil {
		return errors.New("failed to marshal message: " + err.Error())
	}

	// Update memory usage
	msgSize := int64(len(body))
	if msgSize > int64(b.config.MaxMessageSize) {
		return errors.New("message size " + fmt.Sprintf("%d", msgSize) + " exceeds limit " + fmt.Sprintf("%d", b.config.MaxMessageSize))
	}
	b.memUsage.Add(msgSize)
	defer b.memUsage.Add(-msgSize)

	err = ch.PublishWithContext(ctx,
		b.config.Exchange,
		b.config.RoutingKey,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: b.config.DeliveryMode,
			Timestamp:    time.Now(),
		},
	)
	if err != nil {
		return errors.New("failed to publish message: " + err.Error())
	}

	return nil
}

// monitorResources periodically monitors system resources
func (b *RabbitMQBroker) monitorResources() {
	ticker := time.NewTicker(b.config.MonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.done:
			return
		case <-ticker.C:
			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			log.Debug().
				Int64("memory_usage_mb", b.memUsage.Load()/1024/1024).
				Int32("active_workers", b.workerCount.Load()).
				Int64("messages_published", b.stats.messagesPublished.Load()).
				Int64("publish_errors", b.stats.publishErrors.Load()).
				Int64("reconnect_count", b.stats.reconnectCount.Load()).
				Int("queue_size", len(b.messages)).
				Msg("Resource usage stats")

			// Auto-scale workers based on queue size
			queueSize := len(b.messages)
			currentWorkers := int(b.workerCount.Load())
			if queueSize > b.config.MaxQueueSize/2 && currentWorkers < b.config.MaxWorkers {
				b.wg.Add(1)
				b.workerCount.Add(1)
				go b.worker()
			}
		}
	}
}

// Metrics returns current broker statistics
func (b *RabbitMQBroker) Metrics() broker.BrokerMetrics {
	var lastError error
	if errStr := b.stats.lastError.Load(); errStr != nil {
		lastError = errors.New(errStr.(string))
	}

	return broker.BrokerMetrics{
		MessagesPublished: b.stats.messagesPublished.Load(),
		MessagesFailed:    b.stats.publishErrors.Load(),
		BufferSize:        len(b.messages),
		LastError:         lastError,
		LastPublishTime:   time.Unix(0, b.stats.lastPublishTime.Load()),
	}
}
