package rabbitmq

import (
	"fmt"
	"time"
)

// Config holds RabbitMQ broker configuration
type Config struct {
	// Connection settings
	URI             string        `koanf:"uri"`
	ConnectTimeout  time.Duration `koanf:"connect_timeout"`
	RequestTimeout  time.Duration `koanf:"request_timeout"`
	MaxConnections  int           `koanf:"max_connections"`
	MaxChannels     int           `koanf:"max_channels_per_conn"`
	HeartbeatDelay  time.Duration `koanf:"heartbeat_delay"`
	ConnectionRetry int           `koanf:"connection_retry"`

	// Resource limits
	MaxMemoryMB      int           `koanf:"max_memory_mb"`
	MaxMessageSize   int           `koanf:"max_message_size"`
	MaxWorkers       int           `koanf:"max_workers"`
	MaxQueueSize     int           `koanf:"max_queue_size"`
	ShutdownTimeout  time.Duration `koanf:"shutdown_timeout"`
	IdleTimeout      time.Duration `koanf:"idle_timeout"`
	CleanupInterval  time.Duration `koanf:"cleanup_interval"`
	MonitorInterval  time.Duration `koanf:"monitor_interval"`

	// Exchange and queue settings
	Exchange         string `koanf:"exchange"`
	ExchangeType    string `koanf:"exchange_type"`
	RoutingKey      string `koanf:"routing_key"`
	Queue           string `koanf:"queue"`
	Durable         bool   `koanf:"durable"`
	AutoDelete      bool   `koanf:"auto_delete"`
	Exclusive       bool   `koanf:"exclusive"`
	NoWait          bool   `koanf:"no_wait"`
	DeliveryMode    uint8  `koanf:"delivery_mode"`
	PublishTimeout  time.Duration `koanf:"publish_timeout"`
}

// DefaultConfig returns a Config with default values
func DefaultConfig() Config {
	return Config{
		ConnectTimeout:   time.Second * 30,
		RequestTimeout:   time.Second * 10,
		MaxConnections:   5,
		MaxChannels:      10,
		HeartbeatDelay:   time.Second * 10,
		ConnectionRetry:  3,
		MaxMemoryMB:      1024,
		MaxMessageSize:   1024 * 1024, // 1MB
		MaxWorkers:       10,
		MaxQueueSize:     10000,
		ShutdownTimeout:  time.Second * 30,
		IdleTimeout:      time.Minute * 5,
		CleanupInterval:  time.Minute,
		MonitorInterval:  time.Second * 30,
		Exchange:         "walstreamer",
		ExchangeType:     "topic",
		RoutingKey:       "changes.*",
		Queue:            "walstreamer.changes",
		Durable:          true,
		AutoDelete:       false,
		Exclusive:        false,
		NoWait:          false,
		DeliveryMode:     2, // persistent
		PublishTimeout:   time.Second * 5,
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.URI == "" {
		return fmt.Errorf("uri is required")
	}
	if c.MaxConnections <= 0 {
		return fmt.Errorf("max connections must be greater than 0")
	}
	if c.MaxChannels <= 0 {
		return fmt.Errorf("max channels per connection must be greater than 0")
	}
	if c.MaxMemoryMB <= 0 {
		return fmt.Errorf("max memory must be greater than 0")
	}
	if c.MaxMessageSize <= 0 {
		return fmt.Errorf("max message size must be greater than 0")
	}
	if c.MaxWorkers <= 0 {
		return fmt.Errorf("max workers must be greater than 0")
	}
	if c.MaxQueueSize <= 0 {
		return fmt.Errorf("max queue size must be greater than 0")
	}
	if c.Exchange == "" {
		return fmt.Errorf("exchange is required")
	}
	if c.RoutingKey == "" {
		return fmt.Errorf("routing key is required")
	}
	if c.Queue == "" {
		return fmt.Errorf("queue is required")
	}
	return nil
}

// URL returns the AMQP URL for the configuration
func (c *Config) URL() string {
	return c.URI
}
