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

// DatabaseConfig holds PostgreSQL connection configuration
type DatabaseConfig struct {
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	User     string `koanf:"user"`
	Password string `koanf:"password"`
	DBName   string `koanf:"dbname"`
	SSLMode  string `koanf:"sslmode"`
}

// TableConfig defines configuration for a specific table
type TableConfig struct {
	Name       string   `yaml:"name"`       // Full table name (schema.table)
	Operations []string `yaml:"operations"` // List of operations to listen for (INSERT, UPDATE, DELETE)
}

// ReplicationConfig holds replication-related configuration
type ReplicationConfig struct {
	PublicationName string        `koanf:"publication_name"`
	SlotName        string        `koanf:"slot_name"`
	StandbyTimeout  int           `koanf:"standby_timeout"`
	Tables          []string      `koanf:"tables"`        // For backward compatibility
	TableConfigs    []TableConfig `koanf:"table_configs"` // New table-specific configurations
	DefaultOps      []string      `koanf:"default_ops"`   // Default operations for tables without specific config
	InitialSync     bool          `koanf:"initial_sync"`  // Whether to sync existing data on first run
	BatchSize       int           `koanf:"batch_size"`    // Batch size for initial sync, default 1000
}

func (c *ReplicationConfig) Validate() error {
	if c.PublicationName == "" {
		return fmt.Errorf("publication_name is required")
	}
	if c.SlotName == "" {
		return fmt.Errorf("slot_name is required")
	}
	if c.StandbyTimeout <= 0 {
		return fmt.Errorf("standby_timeout must be positive")
	}

	// Validate operations
	validOps := map[string]bool{"INSERT": true, "UPDATE": true, "DELETE": true}

	// Validate default operations
	for _, op := range c.DefaultOps {
		if !validOps[op] {
			return fmt.Errorf("invalid default operation: %s", op)
		}
	}

	// If no default operations specified, use all
	if len(c.DefaultOps) == 0 {
		c.DefaultOps = []string{"INSERT", "UPDATE", "DELETE"}
	}

	// Validate table configs
	for _, tc := range c.TableConfigs {
		if tc.Name == "" {
			return fmt.Errorf("table name is required in table_configs")
		}
		if len(tc.Operations) == 0 {
			tc.Operations = c.DefaultOps
		} else {
			for _, op := range tc.Operations {
				if !validOps[op] {
					return fmt.Errorf("invalid operation '%s' for table %s", op, tc.Name)
				}
			}
		}
	}

	if c.BatchSize <= 0 {
		c.BatchSize = 1000 // Set default batch size
	}

	return nil
}

// BrokerConfig holds message broker configuration
type BrokerConfig struct {
	Type     string   `koanf:"type"` // inmemory, kafka, nats
	Hosts    []string `koanf:"hosts"`
	Topic    string   `koanf:"topic"`
	Username string   `koanf:"username"`
	Password string   `koanf:"password"`
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
