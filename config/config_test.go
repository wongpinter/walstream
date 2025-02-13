package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	// Create temp config file
	tmpDir, err := os.MkdirTemp("", "config_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	configFile := filepath.Join(tmpDir, "config.yaml")
	configContent := []byte(`
database:
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  dbname: postgres
  sslmode: disable

replication:
  slot_name: walstreamer_slot
  publication_name: walstreamer_pub
  included_tables: []
  excluded_tables: []
  standby_timeout: 10s

broker:
  type: inmemory
  hosts: []
  topic: walstreamer
  username: ""
  password: ""

lsn:
  type: file
  path: ./data/lsn
  persist_interval: 5s

log:
  level: info
  format: json
`)

	if err := os.WriteFile(configFile, configContent, 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Test loading from file
	cfg, err := Load([]string{configFile})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verify loaded values
    if cfg.Database.Host != "localhost" {
        t.Errorf("expected host localhost, got %s", cfg.Database.Host)
    }
    if cfg.Database.Port != 5432 {
        t.Errorf("expected port 5432, got %d", cfg.Database.Port)
    }
    if cfg.Replication.Slot != "walstreamer_slot" {
        t.Errorf("expected slot walstreamer_slot, got %s", cfg.Replication.Slot)
    }
    if cfg.Replication.StandbyTimeout != 10 {
        t.Errorf("expected standby_timeout 10, got %v", cfg.Replication.StandbyTimeout)
    }
    if cfg.Storage.Type != "file" {
        t.Errorf("expected storage type file, got %s", cfg.Storage.Type)
    }

	// Test environment variable override
	os.Setenv("WALSTREAMER_DATABASE_HOST", "testhost")
	os.Setenv("WALSTREAMER_DATABASE_PORT", "5433")
	defer func() {
		os.Unsetenv("WALSTREAMER_DATABASE_HOST")
		os.Unsetenv("WALSTREAMER_DATABASE_PORT")
	}()

	cfg, err = Load([]string{configFile})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Database.Host != "testhost" {
		t.Errorf("expected host testhost, got %s", cfg.Database.Host)
	}
	if cfg.Database.Port != 5433 {
		t.Errorf("expected port 5433, got %d", cfg.Database.Port)
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				Database: DatabaseConfig{
					Host:     "localhost",
					Port:     5432,
					User:     "postgres",
					Password: "postgres",
					DBName:   "postgres",
					SSLMode:  "disable",
				},
				Replication: ReplicationConfig{
					Slot:           "walstreamer_slot",
					Publication:    "walstreamer_pub",
					StandbyTimeout: 10,
                    InitialSync:    false,
                    BatchSize: 1000,
                    Reconnect: ReconnectConfig{
                        MaxAttempts:  3,
                        InitialDelay: 5,
                    },
                    SchemaName: "public",
				},
                Broker: BrokerConfig{
                    Type:  "inmemory",
                    Topic: "walstreamer",
                },
                Storage: StorageConfig{
                    Type: "file",
                },
				LSN: LSNConfig{
					Type:            "file",
					Path:            "./data/lsn",
					PersistInterval: Duration(5 * time.Second),
				},
				Log: LogConfig{
					Level:  "info",
					Format: "json",
				},
			},
			wantErr: false,
		},
        {
            name: "invalid database config",
            cfg: Config{
                Database: DatabaseConfig{
                    Host: "", // Missing host
                },
            },
            wantErr: true,
        },
        {
            name: "invalid storage type",
            cfg: Config{
                Database: DatabaseConfig{
                    Host:     "localhost",
                    Port:     5432,
                    User:     "postgres",
                    Password: "postgres",
                    DBName:   "postgres",
                    SSLMode:  "disable",
                },
                Storage: StorageConfig{
                    Type: "invalid",
                },
            },
            wantErr: true,
        },
		{
			name: "invalid broker type",
			cfg: Config{
				Database: DatabaseConfig{
					Host:     "localhost",
					Port:     5432,
					User:     "postgres",
					Password: "postgres",
					DBName:   "postgres",
					SSLMode:  "disable",
				},
				Broker: BrokerConfig{
					Type: "invalid",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
