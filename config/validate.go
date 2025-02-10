package config

import (
	"fmt"
	"time"
)

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if err := c.Database.Validate(); err != nil {
		return fmt.Errorf("database config: %w", err)
	}
	if err := c.Replication.Validate(); err != nil {
		return fmt.Errorf("replication config: %w", err)
	}
	if err := c.Broker.Validate(); err != nil {
		return fmt.Errorf("broker config: %w", err)
	}
	if err := c.LSN.Validate(); err != nil {
		return fmt.Errorf("lsn config: %w", err)
	}
	if err := c.Log.Validate(); err != nil {
		return fmt.Errorf("log config: %w", err)
	}
	return nil
}

// Validate checks if the database configuration is valid
func (c *DatabaseConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.Port <= 0 {
		return fmt.Errorf("port must be positive")
	}
	if c.User == "" {
		return fmt.Errorf("user is required")
	}
	if c.Password == "" {
		return fmt.Errorf("password is required")
	}
	if c.DBName == "" {
		return fmt.Errorf("dbname is required")
	}
	if c.SSLMode == "" {
		return fmt.Errorf("sslmode is required")
	}
	return nil
}

// Validate checks if the LSN configuration is valid
func (c *LSNConfig) Validate() error {
	switch c.Type {
	case "file":
		if c.Path == "" {
			return fmt.Errorf("path is required for file LSN storage")
		}
	case "postgres":
		// Additional validation for postgres LSN storage if needed
	default:
		return fmt.Errorf("unsupported LSN storage type: %s", c.Type)
	}
	if time.Duration(c.PersistInterval) < time.Second {
		return fmt.Errorf("persist_interval must be at least 1s")
	}
	return nil
}

// Validate checks if the log configuration is valid
func (c *LogConfig) Validate() error {
	switch c.Level {
	case "debug", "info", "warn", "error":
		// Valid log levels
	default:
		return fmt.Errorf("unsupported log level: %s", c.Level)
	}
	switch c.Format {
	case "json", "console":
		// Valid log formats
	default:
		return fmt.Errorf("unsupported log format: %s", c.Format)
	}
	return nil
}
