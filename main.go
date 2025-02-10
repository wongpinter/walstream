package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"repo.nusatek.id/sugeng/walstreamer/broker"
	"repo.nusatek.id/sugeng/walstreamer/broker/inmemory"
	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/lsn"
	"repo.nusatek.id/sugeng/walstreamer/model"
	"repo.nusatek.id/sugeng/walstreamer/replication"
	"repo.nusatek.id/sugeng/walstreamer/wal"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	// Configure logging
	level, err := zerolog.ParseLevel(cfg.Log.Level)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid log level: %v\n", err)
		os.Exit(1)
	}
	zerolog.SetGlobalLevel(level)

	if cfg.Log.Format == "console" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	// Initialize in-memory broker with configuration
	brokerConfig := broker.BrokerConfig{
		BufferSize: 1000, // Configurable buffer size
		BatchConfig: broker.BatchConfig{
			Size:          100,
			Workers:       2,
			FlushInterval: 5 * time.Second,
		},
		ShutdownTimeout:  30 * time.Second,
		HeartbeatTimeout: 5 * time.Second,
	}
	broker := inmemory.NewInMemoryBroker(brokerConfig)

	// Create LSN storage
	storage, err := lsn.NewFileStorage(cfg.LSN.Path, time.Duration(cfg.LSN.PersistInterval)*time.Second)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create LSN storage")
	}
	defer storage.Close()

	// Setup replication
	replConfig := replication.Config{
		PublicationName: cfg.Replication.PublicationName,
		SlotName:        cfg.Replication.SlotName,
		IncludedTables:  []string{}, // Empty means all tables
		ExcludedTables:  []string{}, // Parse excluded tables from cfg.Replication.Tables
	}

	// Parse table filters
	for _, pattern := range cfg.Replication.Tables {
		if strings.HasPrefix(pattern, "!") {
			replConfig.ExcludedTables = append(replConfig.ExcludedTables, strings.TrimPrefix(pattern, "!"))
		} else if pattern != "" {
			replConfig.IncludedTables = append(replConfig.IncludedTables, pattern)
		}
	}

	replManager := replication.NewManager(replConfig)
	if err := replManager.Connect(context.Background(), cfg.Database.GetDSN()); err != nil {
		log.Fatal().Err(err).Msg("Failed to connect replication manager")
	}
	defer replManager.Close(context.Background())

	if err := replManager.Setup(context.Background()); err != nil {
		log.Fatal().Err(err).Msg("Failed to setup replication")
	}

	// Create WAL reader config
	walConfig := wal.Config{
		ConnString:      cfg.Database.GetReplicationDSN(),
		PublicationName: cfg.Replication.PublicationName,
		SlotName:        cfg.Replication.SlotName,
		StandbyTimeout:  time.Duration(cfg.Replication.StandbyTimeout) * time.Second,
		Logger:          log.Logger,
		LSNStorage:      storage,
	}

	// Create message handler that publishes to broker
	messageHandler := func(msg *model.Message) error {
		// Check if table is in filter list
		tableFullName := fmt.Sprintf("%s.%s", msg.Schema, msg.Table)

		// First check exclusions
		for _, pattern := range cfg.Replication.Tables {
			if strings.HasPrefix(pattern, "!") && tableFullName == strings.TrimPrefix(pattern, "!") {
				return nil // Skip excluded table
			}
		}

		// Check if table has specific configuration
		var allowedOps []string
		for _, tc := range cfg.Replication.TableConfigs {
			if tc.Name == tableFullName {
				allowedOps = tc.Operations
				break
			}
		}

		// If no specific config found, check if table is in general include list
		if allowedOps == nil {
			tableIncluded := len(cfg.Replication.Tables) == 0 // Empty list means include all
			for _, pattern := range cfg.Replication.Tables {
				if !strings.HasPrefix(pattern, "!") && (pattern == "" || pattern == tableFullName) {
					tableIncluded = true
					break
				}
			}
			if !tableIncluded {
				return nil // Skip table not in include list
			}
			allowedOps = cfg.Replication.DefaultOps
		}

		// Check if operation is allowed
		operationAllowed := false
		for _, op := range allowedOps {
			if op == msg.Operation {
				operationAllowed = true
				break
			}
		}
		if !operationAllowed {
			log.Debug().
				Str("operation", msg.Operation).
				Str("table", tableFullName).
				Strs("allowed_ops", allowedOps).
				Msg("skipping message due to operation filter")
			return nil
		}

		// Print message details for debugging
		log.Debug().
			Str("operation", msg.Operation).
			Str("schema", msg.Schema).
			Str("table", msg.Table).
			Uint64("lsn", msg.LSN).
			Int64("timestamp", msg.Timestamp.Unix()).
			Interface("before", msg.Before).
			Interface("after", msg.After).
			Msg("received WAL message")

		// Publish message to broker
		if err := broker.Publish(context.Background(), msg); err != nil {
			return fmt.Errorf("failed to publish message: %w", err)
		}

		// Print broker metrics for debugging
		metrics := broker.Metrics()
		log.Info().
			Int64("messages_published", metrics.MessagesPublished).
			Int64("messages_failed", metrics.MessagesFailed).
			Int("buffer_size", metrics.BufferSize).
			Msg("broker metrics")

		return nil
	}

	// Create WAL reader
	reader, err := wal.NewReader(walConfig, messageHandler)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create WAL reader")
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		cancel()
	}()

	// Start WAL reader
	log.Info().
		Str("slot", cfg.Replication.SlotName).
		Str("publication", cfg.Replication.PublicationName).
		Msg("Starting WAL reader")

	if err := reader.Start(ctx); err != nil {
		log.Fatal().Err(err).Msg("WAL reader failed")
	}
}
