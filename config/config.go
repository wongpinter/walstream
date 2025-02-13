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

// TableConfig holds configuration for a specific table
type TableConfig struct {
	Operations []string `koanf:"operations"` // List of operations to capture
	Topic      string   `koanf:"topic"`      // Optional topic override for this table
	Columns    []string `koanf:"columns"`    // List of columns to capture
}

// ReplicationConfig holds configuration for WAL replication
type ReplicationConfig struct {
	Publication    string                 `koanf:"publication"`     // PostgreSQL publication name
	Slot           string                 `koanf:"slot"`            // PostgreSQL replication slot name
	StandbyTimeout int                    `koanf:"standby_timeout"` // Timeout for standby status updates (in seconds)
	InitialSync    bool                   `koanf:"initial_sync"`    // Whether to perform initial table sync
	BatchSize      int                    `koanf:"batch_size"`      // Number of changes to process in a batch
	Reconnect      ReconnectConfig        `koanf:"reconnect"`       // Reconnection settings
	SchemaName     string                 `koanf:"schema_name"`     // Schema name for tables
	Tables         map[string]TableConfig `koanf:"tables"`          // Map of tables to replicate
}

// GetTableNames returns a list of all table names in the replication configuration
func (c *ReplicationConfig) GetTableNames() []string {
	tableNames := make([]string, 0, len(c.Tables))
	for tableName := range c.Tables {
		tableNames = append(tableNames, tableName)
	}
	return tableNames
}

// GetColumnsForTable returns the list of columns for a specific table
func (c *ReplicationConfig) GetColumnsForTable(tableName string) ([]string, error) {
	tableConfig, exists := c.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s not found in replication configuration", tableName)
	}
	return tableConfig.Columns, nil
}

// IsTableReplicated checks if a table is included in the replication configuration
func (c *ReplicationConfig) IsTableReplicated(tableName string) bool {
	_, exists := c.Tables[tableName]
	return exists
}

// GetTableConfig returns the configuration for a specific table
func (c *ReplicationConfig) GetTableConfig(tableName string) (*TableConfig, error) {
	tableConfig, exists := c.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s not found in replication configuration", tableName)
	}
	return &tableConfig, nil
}

// getTableOperations returns the list of operations for a specific table
func (c *ReplicationConfig) GetTableOperations(tableName string) ([]string, error) {
	tableConfig, exists := c.Tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s not found in replication configuration", tableName)
	}
	return tableConfig.Operations, nil
}

// IsOperationAllowed checks if a specific operation is allowed for a table
func (c *ReplicationConfig) IsOperationAllowed(tableName string, operation string) (bool, error) {
	tableConfig, exists := c.Tables[tableName]
	if !exists {
		return false, fmt.Errorf("table %s not found in replication configuration", tableName)
	}
	for _, op := range tableConfig.Operations {
		if op == operation {
			return true, nil
		}
	}
	return false, nil
}

// ReconnectConfig holds reconnection settings
type ReconnectConfig struct {
	MaxAttempts  int `koanf:"max_attempts"`  // Maximum reconnection attempts (0 means unlimited)
	InitialDelay int `koanf:"initial_delay"` // Initial delay between reconnection attempts (in seconds)
	MaxDelay     int `koanf:"max_delay"`     // Maximum delay between reconnection attempts (in seconds)
}

// BrokerConfig holds broker-related configuration
type BrokerConfig struct {
	Type   string       `koanf:"type"`   // Broker type (inmemory, nats, pubsub)
	Topic  string       `koanf:"topic"`  // Default topic/subject name
	PubSub PubSubConfig `koanf:"pubsub"` // Google Cloud Pub/Sub specific configuration
}

// PubSubConfig holds Google Cloud Pub/Sub specific configuration
type PubSubConfig struct {
	ProjectID       string `koanf:"project_id"`        // Google Cloud project ID
	TopicPrefix     string `koanf:"topic_prefix"`      // Prefix for auto-generated topics
	CredentialsFile string `koanf:"credentials_file"`  // Path to JSON credentials file
	AutoCreateTopic bool   `koanf:"auto_create_topic"` // Whether to automatically create topics
	Location        string `koanf:"location"`          // Location for BigQuery tables
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

// Load loads configuration from multiple YAML files and environment variables
func Load(configFiles []string) (*Config, error) {
	k := koanf.New(".")

	// Load each YAML file
	for _, configFile := range configFiles {
		if err := k.Load(file.Provider(configFile), yaml.Parser()); err != nil {
			return nil, fmt.Errorf("error loading config file %s: %w", configFile, err)
		}
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

// GetIncludedTables returns a list of included tables
func (cfg Config) GetIncludedTables() []string {
	includedTables := make([]string, 0, len(cfg.Replication.Tables))
	for tableName := range cfg.Replication.Tables {
		includedTables = append(includedTables, tableName)
	}
	return includedTables
}

// Validate checks if the replication configuration is valid
func (c *ReplicationConfig) Validate() error {
	if c.Publication == "" {
		return fmt.Errorf("publication name is required")
	}
	if c.Slot == "" {
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
	for tableName, table := range c.Tables {
		if tableName == "" {
			return fmt.Errorf("table name is required")
		}
		if len(table.Operations) == 0 {
			return fmt.Errorf("operations must be specified for table %s", tableName)
		}
	}

	return nil
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
		return fmt.Errorf("NATS configuration is not supported in this version")
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
