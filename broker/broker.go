package broker

import (
	"context"
	"errors"
	"time"

	"repo.nusatek.id/sugeng/walstreamer/model"
)

var (
	// ErrBrokerClosed is returned when the broker is closed
	ErrBrokerClosed = errors.New("broker is closed")
)

// MessageValidator is a function type that validates a message before publishing
type MessageValidator func(*model.Message) error

// MessageTransformer is a function type that transforms a message before publishing
type MessageTransformer func(*model.Message) (*model.Message, error)

// RetryPolicy defines the retry behavior for failed publish attempts
type RetryPolicy struct {
	MaxAttempts     int
	InitialInterval time.Duration
	MaxInterval     time.Duration
	Multiplier      float64
	RandomFactor    float64
}

// BatchConfig defines the configuration for batch processing
type BatchConfig struct {
	Size          int           // Maximum number of messages in a batch
	FlushInterval time.Duration // Maximum time to wait before flushing a batch
	Workers       int           // Number of worker goroutines for processing batches
}

// BrokerConfig holds the configuration for a broker
type BrokerConfig struct {
	RetryPolicy      RetryPolicy
	BatchConfig      BatchConfig
	Validators       []MessageValidator
	Transformers     []MessageTransformer
	Format           model.MessageFormat
	BufferSize       int           // Size of the message buffer channel
	ShutdownTimeout  time.Duration // Maximum time to wait for graceful shutdown
	HeartbeatTimeout time.Duration // Maximum time to wait for broker heartbeat
}

// Broker defines the interface for message brokers
type Broker interface {
	// Publish publishes a message to the broker
	Publish(ctx context.Context, message *model.Message) error

	// PublishBatch publishes a batch of messages to the broker
	PublishBatch(ctx context.Context, messages []*model.Message) error

	// AddValidator adds a message validator to the broker
	AddValidator(validator MessageValidator)

	// AddTransformer adds a message transformer to the broker
	AddTransformer(transformer MessageTransformer)

	// Flush forces any buffered messages to be sent
	Flush(ctx context.Context) error

	// Health returns the current health status of the broker
	Health(ctx context.Context) error

	// Metrics returns the current metrics of the broker
	Metrics() BrokerMetrics

	// Close closes the broker connection
	Close() error
}

// BrokerMetrics holds metrics about the broker
type BrokerMetrics struct {
	MessagesPublished  int64
	MessagesFailed     int64
	BatchesPublished   int64
	BatchesFailed      int64
	AverageLatency     time.Duration
	BufferSize         int
	LastPublishTime    time.Time
	LastSuccessfulTime time.Time
	LastErrorTime      time.Time
	LastError          error
}
