package rabbitmq

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

// newConnectionPool creates a new connection pool
func newConnectionPool(cfg Config) (*connectionPool, error) {
	pool := &connectionPool{
		connections: make([]*amqp.Connection, 0, cfg.MaxConnections),
		channels:    make(map[*amqp.Connection][]*amqp.Channel),
		config:      cfg,
	}

	// Create initial connection
	if err := pool.addConnection(); err != nil {
		return nil, err
	}

	return pool, nil
}

// addConnection adds a new connection to the pool
func (p *connectionPool) addConnection() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.connections) >= p.config.MaxConnections {
		return fmt.Errorf("connection pool full (max: %d)", p.config.MaxConnections)
	}

	conn, err := amqp.DialConfig(p.config.URI, amqp.Config{
		Heartbeat: p.config.HeartbeatDelay,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	p.connections = append(p.connections, conn)
	p.channels[conn] = make([]*amqp.Channel, 0, p.config.MaxChannels)

	// Monitor connection
	go p.monitorConnection(conn)

	return nil
}

// getChannel gets an available channel from the pool
func (p *connectionPool) getChannel() (*amqp.Channel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Try to find an available connection
	for _, conn := range p.connections {
		if len(p.channels[conn]) < p.config.MaxChannels {
			ch, err := conn.Channel()
			if err != nil {
				log.Error().Err(err).Msg("Failed to create channel")
				continue
			}

			// Configure channel
			if err := ch.Confirm(false); err != nil {
				ch.Close()
				return nil, fmt.Errorf("failed to put channel in confirm mode: %w", err)
			}

			// Declare exchange
			if err := ch.ExchangeDeclare(
				p.config.Exchange,
				p.config.ExchangeType,
				p.config.Durable,
				p.config.AutoDelete,
				false, // internal
				p.config.NoWait,
				nil,
			); err != nil {
				ch.Close()
				return nil, fmt.Errorf("failed to declare exchange: %w", err)
			}

			// Declare queue
			if _, err := ch.QueueDeclare(
				p.config.Queue,
				p.config.Durable,
				p.config.AutoDelete,
				p.config.Exclusive,
				p.config.NoWait,
				nil,
			); err != nil {
				ch.Close()
				return nil, fmt.Errorf("failed to declare queue: %w", err)
			}

			// Bind queue
			if err := ch.QueueBind(
				p.config.Queue,
				p.config.RoutingKey,
				p.config.Exchange,
				p.config.NoWait,
				nil,
			); err != nil {
				ch.Close()
				return nil, fmt.Errorf("failed to bind queue: %w", err)
			}

			p.channels[conn] = append(p.channels[conn], ch)
			return ch, nil
		}
	}

	// All connections are at capacity, try to add a new one
	if err := p.addConnection(); err != nil {
		return nil, fmt.Errorf("failed to add connection: %w", err)
	}

	// Retry with new connection
	return p.getChannel()
}

// monitorConnection monitors a connection for closure
func (p *connectionPool) monitorConnection(conn *amqp.Connection) {
	closeErr := <-conn.NotifyClose(make(chan *amqp.Error))
	log.Error().Err(closeErr).Msg("RabbitMQ connection closed")

	p.mu.Lock()
	defer p.mu.Unlock()

	// Remove closed connection and its channels
	for i, c := range p.connections {
		if c == conn {
			p.connections = append(p.connections[:i], p.connections[i+1:]...)
			delete(p.channels, conn)
			break
		}
	}

	// Try to replace the connection
	go func() {
		for i := 0; i < p.config.ConnectionRetry; i++ {
			if err := p.addConnection(); err != nil {
				log.Error().Err(err).Int("attempt", i+1).Msg("Failed to reconnect")
				time.Sleep(time.Second * time.Duration(i+1)) // Exponential backoff
				continue
			}
			log.Info().Msg("Successfully reconnected to RabbitMQ")
			return
		}
		log.Error().Msg("Failed to reconnect after multiple attempts")
	}()
}

// close closes all connections in the pool
func (p *connectionPool) close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var lastErr error
	for conn, channels := range p.channels {
		for _, ch := range channels {
			if err := ch.Close(); err != nil {
				lastErr = err
				log.Error().Err(err).Msg("Failed to close channel")
			}
		}
		if err := conn.Close(); err != nil {
			lastErr = err
			log.Error().Err(err).Msg("Failed to close connection")
		}
	}

	p.connections = nil
	p.channels = make(map[*amqp.Connection][]*amqp.Channel)

	return lastErr
}
