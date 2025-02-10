package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Database struct {
		Host     string `yaml:"host"`
		Port     int    `yaml:"port"`
		User     string `yaml:"user"`
		Password string `yaml:"password"`
		DBName   string `yaml:"dbname"`
	} `yaml:"database"`
	Replication struct {
		SlotName    string `yaml:"slot_name"`
		Publication string `yaml:"publication"`
	} `yaml:"replication"`
	Broker struct {
		Type    string   `yaml:"type"` // inmemory, kafka, nats
		Address []string `yaml:"address"`
	} `yaml:"broker"`
	Storage struct {
		Type string `yaml:"type"`
		Path string `yaml:"path"`
	} `yaml:"storage"`
}

func LoadConfig(path string) (*Config, error) {
	cfg := &Config{}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return cfg, nil
}
