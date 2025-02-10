package rabbitmq

import (
	"fmt"
	"time"
)

// Config holds RabbitMQ-specific configuration
type Config struct {
	// Connection settings
	Host     string
	Port     int
	Username string
	Password string
	VHost    string

	// Exchange settings
	ExchangeName string
	ExchangeType string // "direct", "fanout", "topic", "headers"
	Durable      bool
	AutoDelete   bool

	// Queue settings
	QueueName       string
	RoutingKey      string
	QueueDurable    bool
	QueueAutoDelete bool
	Exclusive       bool

	// Channel settings
	ChannelBufferSize int
	PrefetchCount     int
	PrefetchSize      int
	PrefetchGlobal    bool

	// Publisher settings
	Mandatory   bool
	Immediate   bool
	Persistent  bool
	ContentType string

	// Connection management
	ReconnectDelay    time.Duration
	ConnectionTimeout time.Duration
}

// DefaultConfig returns a Config with default values
func DefaultConfig() Config {
	return Config{
		Host:              "localhost",
		Port:             5672,
		VHost:            "/",
		ExchangeType:     "topic",
		Durable:          true,
		AutoDelete:       false,
		QueueDurable:     true,
		QueueAutoDelete:  false,
		Exclusive:        false,
		ChannelBufferSize: 1000,
		PrefetchCount:    1,
		PrefetchGlobal:   false,
		Persistent:       true,
		ContentType:      "application/json",
		ReconnectDelay:   time.Second * 5,
		ConnectionTimeout: time.Second * 30,
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port number")
	}
	if c.ExchangeName == "" {
		return fmt.Errorf("exchange name is required")
	}
	if c.QueueName == "" {
		return fmt.Errorf("queue name is required")
	}
	if c.RoutingKey == "" {
		return fmt.Errorf("routing key is required")
	}
	return nil
}

// URL returns the AMQP URL for the configuration
func (c *Config) URL() string {
	auth := ""
	if c.Username != "" || c.Password != "" {
		auth = fmt.Sprintf("%s:%s@", c.Username, c.Password)
	}
	return fmt.Sprintf("amqp://%s%s:%d%s", auth, c.Host, c.Port, c.VHost)
}
