package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

// Config holds all configuration for the walstreamer
type Config struct {
	Database    DatabaseConfig    `koanf:"database"`
	Replication ReplicationConfig `koanf:"replication"`
	Broker      BrokerConfig      `koanf:"broker"`
	LSN         LSNConfig         `koanf:"lsn"`
	Log         LogConfig         `koanf:"log"`
	Storage     StorageConfig     `koanf:"storage"`
}

func (cfg Config) GetIncludedTables() []string {
	includedTables := make([]string, 0)
	for _, table := range cfg.Replication.Tables {
		if !strings.HasPrefix(table.Name, "!") && table.Name != "" {
			includedTables = append(includedTables, table.Name)
		}
	}

	return includedTables
}

func (cfg Config) GetExcludedTables() []string {
	excludedTables := make([]string, 0)
	for _, table := range cfg.Replication.Tables {
		if strings.HasPrefix(table.Name, "!") {
			excludedTables = append(excludedTables, strings.TrimPrefix(table.Name, "!"))
		}
	}

	return excludedTables
}

// DatabaseConfig holds PostgreSQL connection configuration
type DatabaseConfig struct {
	Schema   string `koanf:"schema"`
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	DBName   string `koanf:"dbname"`
	SSLMode  string `koanf:"sslmode"`
}

// TableConfig holds configuration for a specific table
type TableConfig struct {
	Name       string   `koanf:"name"`       // Table name in format schema.table
	Operations []string `koanf:"operations"` // List of operations to capture
	Topic      string   `koanf:"topic"`      // Optional topic override for this table
}

// ReplicationConfig holds configuration for WAL replication
type ReplicationConfig struct {
	PublicationName string        `koanf:"publication_name"` // PostgreSQL publication name
	SlotName        string        `koanf:"slot_name"`        // PostgreSQL replication slot name
	StandbyTimeout  int           `koanf:"standby_timeout"`  // Timeout for standby status updates (in seconds)
	Tables          []TableConfig `koanf:"tables"`           // List of tables to replicate
	DefaultOps      []string      `koanf:"default_ops"`      // Default operations to capture
	InitialSync     bool          `koanf:"initial_sync"`     // Whether to perform initial table sync
	BatchSize       int           `koanf:"batch_size"`       // Number of changes to process in a batch
	Reconnect       struct {
		MaxAttempts  int `koanf:"max_attempts"`  // Maximum reconnection attempts (0 means unlimited)
		InitialDelay int `koanf:"initial_delay"` // Initial delay between reconnection attempts (in seconds)
		MaxDelay     int `koanf:"max_delay"`     // Maximum delay between reconnection attempts (in seconds)
	} `koanf:"reconnect"`
}

func (c *ReplicationConfig) Validate() error {
	if c.PublicationName == "" {
		return fmt.Errorf("publication name is required")
	}
	if c.SlotName == "" {
		return fmt.Errorf("slot name is required")
	}
	if c.StandbyTimeout <= 0 {
		return fmt.Errorf("standby timeout must be positive")
	}
	if len(c.Tables) == 0 {
		return fmt.Errorf("at least one table must be configured")
	}
	if c.BatchSize <= 0 {
		return fmt.Errorf("batch size must be positive")
	}

	// Validate each table configuration
	for _, table := range c.Tables {
		if table.Name == "" {
			return fmt.Errorf("table name is required")
		}
		if len(table.Operations) == 0 && len(c.DefaultOps) == 0 {
			return fmt.Errorf("operations must be specified either in table config or default_ops")
		}
	}

	return nil
}

// BrokerConfig holds broker-related configuration
type BrokerConfig struct {
	Type          string   `koanf:"type"`           // Broker type (inmemory, nats, pubsub)
	Hosts         []string `koanf:"hosts"`          // List of broker hosts
	Topic         string   `koanf:"topic"`          // Default topic/subject name
	Username      string   `koanf:"username"`       // Optional username for authentication
	Password      string   `koanf:"password"`       // Optional password for authentication
	WriteMetadata bool     `koanf:"write_metadata"` // Optional flag to write metadata
	PubSub        struct {
		ProjectID       string `koanf:"project_id"`        // Google Cloud project ID
		TopicPrefix     string `koanf:"topic_prefix"`      // Prefix for auto-generated topics (e.g., "walstreamer-")
		CredentialsFile string `koanf:"credentials_file"`  // Path to JSON credentials file
		AutoCreateTopic bool   `koanf:"auto_create_topic"` // Whether to automatically create topics
		Location        string `koanf:"location"`          // Location for BigQuery tables
	} `koanf:"pubsub"`
}

// Validate checks if the broker configuration is valid
func (c *BrokerConfig) Validate() error {
	if c.Type == "" {
		return fmt.Errorf("broker type is required")
	}

	switch c.Type {
	case "inmemory":
		// No additional validation needed
	case "nats":
		if len(c.Hosts) == 0 {
			return fmt.Errorf("at least one NATS host is required")
		}
		if c.Topic == "" {
			return fmt.Errorf("NATS topic is required")
		}
	case "pubsub":
		if c.PubSub.ProjectID == "" {
			return fmt.Errorf("Google Cloud project ID is required")
		}
		if c.PubSub.CredentialsFile == "" {
			return fmt.Errorf("path to credentials file is required")
		}
	default:
		return fmt.Errorf("unsupported broker type: %s", c.Type)
	}

	return nil
}

// LSNConfig holds LSN persistence configuration
type LSNConfig struct {
	Type            string   `koanf:"type"` // file, postgres
	Path            string   `koanf:"path"`
	PersistInterval Duration `koanf:"persist_interval"`
}

// LogConfig holds logging configuration
type LogConfig struct {
	Level  string `koanf:"level"`  // debug, info, warn, error
	Format string `koanf:"format"` // json, console
}

// StorageConfig holds storage configuration
type StorageConfig struct {
	Type string `koanf:"type"`
	Path string `koanf:"path"`
}

// Duration is a wrapper around time.Duration for YAML unmarshaling
type Duration time.Duration

// UnmarshalText implements encoding.TextUnmarshaler
func (d *Duration) UnmarshalText(text []byte) error {
	duration, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(duration)

	return nil
}

// Load loads configuration from file and environment variables
func Load(configFile string) (*Config, error) {
	k := koanf.New(".")

	// Load from YAML file
	if err := k.Load(file.Provider(configFile), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("error loading config: %w", err)
	}

	// Load from environment variables
	if err := k.Load(env.Provider("WALSTREAMER_", ".", func(s string) string {
		return strings.Replace(strings.ToLower(
			strings.TrimPrefix(s, "WALSTREAMER_")), "_", ".", -1)
	}), nil); err != nil {
		return nil, fmt.Errorf("error loading environment variables: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("error unmarshaling config: %w", err)
	}

	return &cfg, nil
}

// GetDSN returns the PostgreSQL connection string
func (c *DatabaseConfig) GetDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.DBName, c.SSLMode)
}

// GetReplicationDSN returns the PostgreSQL connection string for replication
func (c *DatabaseConfig) GetReplicationDSN() string {
	return fmt.Sprintf("%s&replication=database", c.GetDSN())
}
