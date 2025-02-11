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

	"repo.nusatek.id/sugeng/walstreamer/broker"
	"repo.nusatek.id/sugeng/walstreamer/broker/inmemory"
	"repo.nusatek.id/sugeng/walstreamer/broker/nats"
	"repo.nusatek.id/sugeng/walstreamer/broker/pubsub"
	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
	"repo.nusatek.id/sugeng/walstreamer/lsn"
	"repo.nusatek.id/sugeng/walstreamer/model"
	"repo.nusatek.id/sugeng/walstreamer/replication"
	"repo.nusatek.id/sugeng/walstreamer/sync"
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
	loggerConfig := logging.Config{
		Level:  logging.Level(cfg.Log.Level),
		Format: cfg.Log.Format,
	}
	logger, err := logging.New(loggerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	// Initialize broker based on configuration
	var messageBroker broker.Broker
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

	switch cfg.Broker.Type {
	case "nats":
		natsConfig := nats.Config{
			URL:      cfg.Broker.Hosts[0], // Use first host for now
			Subject:  cfg.Broker.Topic,
			Logger:   logger.Logger,
			Username: cfg.Broker.Username,
			Password: cfg.Broker.Password,
		}
		natsBroker, err := nats.NewBroker(natsConfig)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create NATS broker")
			os.Exit(1)
		}
		messageBroker = natsBroker
	case "pubsub":
		pubsubConfig := pubsub.Config{
			ProjectID:       cfg.Broker.PubSub.ProjectID,
			TopicPrefix:     cfg.Broker.PubSub.TopicPrefix,
			CredentialsFile: cfg.Broker.PubSub.CredentialsFile,
			AutoCreateTopic: cfg.Broker.PubSub.AutoCreateTopic,
			Logger:          logger.Logger,
		}
		pubsubBroker, err := pubsub.NewBroker(pubsubConfig)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create Pub/Sub broker")
			os.Exit(1)
		}
		messageBroker = pubsubBroker
	default:
		messageBroker = inmemory.NewInMemoryBroker(brokerConfig)
		logger.Info().Str("type", cfg.Broker.Type).Msg("Using in-memory broker")
	}

	// Create LSN storage
	storage, err := lsn.NewFileStorage(cfg.LSN.Path, time.Duration(cfg.LSN.PersistInterval)*time.Second)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create LSN storage")
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
	for _, table := range cfg.Replication.Tables {
		if strings.HasPrefix(table.Name, "!") {
			replConfig.ExcludedTables = append(replConfig.ExcludedTables, strings.TrimPrefix(table.Name, "!"))
		} else if table.Name != "" {
			replConfig.IncludedTables = append(replConfig.IncludedTables, table.Name)
		}
	}

	replManager := replication.NewManager(replConfig)
	if err := replManager.Connect(context.Background(), cfg.Database.GetDSN()); err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect replication manager")
	}
	defer replManager.Close(context.Background())

	// Setup replication first
	if err := replManager.Setup(context.Background()); err != nil {
		logger.Fatal().Err(err).Msg("Failed to setup replication")
	}

	// Check if LSN file exists and handle initial sync
	if cfg.Replication.InitialSync {
		lsnExists, err := storage.Exists()
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to check LSN file")
		}

		if !lsnExists {
			logger.Info().Msg("LSN file not found, starting initial sync")
			syncer := sync.NewInitialSyncer(cfg, messageBroker, logger.Logger)
			if err := syncer.Start(context.Background()); err != nil {
				logger.Fatal().Err(err).Msg("failed to perform initial sync")
			}
			// Set initial LSN after sync completes
			if err := storage.Set(cfg.Replication.PublicationName, 0); err != nil {
				logger.Fatal().Err(err).Msg("failed to set initial LSN")
			}
			logger.Info().Msg("initial sync completed")
		}
	}

	// Create WAL reader config
	walConfig := wal.Config{
		ConnString:            cfg.Database.GetReplicationDSN(),
		PublicationName:       cfg.Replication.PublicationName,
		SlotName:              cfg.Replication.SlotName,
		StandbyTimeout:        time.Duration(cfg.Replication.StandbyTimeout) * time.Second,
		Logger:                logger.Logger,
		LSNStorage:            storage,
		MaxReconnectAttempts:  cfg.Replication.Reconnect.MaxAttempts,
		ReconnectInitialDelay: time.Duration(cfg.Replication.Reconnect.InitialDelay) * time.Second,
		ReconnectMaxDelay:     time.Duration(cfg.Replication.Reconnect.MaxDelay) * time.Second,
	}

	// Create message handler that publishes to broker
	messageHandler := func(msg *model.Message) error {
		// Get table name
		tableFullName := msg.Table
		if tableFullName == "" {
			logger.Debug().Msg("skipping message without table name")
			return nil
		}

		// Log table information
		logger.Debug().
			Str("table", tableFullName).
			Strs("patterns", getTableNames(cfg.Replication.Tables)).
			Msg("Checking table patterns")

		// Check if table is allowed
		if !isTableAllowed(cfg, tableFullName) {
			logger.Debug().
				Str("table", tableFullName).
				Msg("Table not in configured list")
			return nil
		}

		// Get operations for the table
		operations := getTableOperations(cfg, tableFullName)
		logger.Debug().
			Str("table", tableFullName).
			Strs("operations", operations).
			Msg("Table operations")

		// Get topic for the table if specified
		topic := getTableTopic(cfg, tableFullName)
		if topic != "" {
			logger.Debug().
				Str("table", tableFullName).
				Str("topic", topic).
				Msg("Using custom topic for table")
		}

		// Check if operation is allowed
		operationAllowed := false
		for _, op := range operations {
			if op == msg.Operation {
				operationAllowed = true
				break
			}
		}
		if !operationAllowed {
			logger.Debug().
				Str("operation", msg.Operation).
				Str("table", tableFullName).
				Strs("allowed_ops", operations).
				Msg("skipping message due to operation filter")
			return nil
		}

		// Print message details for debugging
		logger.Debug().
			Str("operation", msg.Operation).
			Str("schema", msg.Schema).
			Str("table", msg.Table).
			Uint64("lsn", msg.LSN).
			Int64("timestamp", msg.Timestamp.Unix()).
			Interface("before", msg.Before).
			Interface("after", msg.After).
			Msg("received WAL message")

		// Publish message to broker
		if err := messageBroker.Publish(context.Background(), msg); err != nil {
			return fmt.Errorf("failed to publish message: %w", err)
		}

		// Print broker metrics for debugging
		metrics := messageBroker.Metrics()
		logger.Info().
			Int64("messages_published", metrics.MessagesPublished).
			Int64("messages_failed", metrics.MessagesFailed).
			Int("buffer_size", metrics.BufferSize).
			Msg("broker metrics")

		return nil
	}

	// Create WAL reader
	reader, err := wal.NewReader(walConfig, messageHandler)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create WAL reader")
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		cancel()
	}()

	// Start WAL reader
	if err := reader.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("WAL reader failed")
	}
}

// getTablePatterns returns a list of table patterns to include
func getTablePatterns(cfg *config.Config) []string {
	patterns := make([]string, 0, len(cfg.Replication.Tables))
	for _, table := range cfg.Replication.Tables {
		if table.Name != "" {
			patterns = append(patterns, table.Name)
		}
	}
	return patterns
}

// getTableOperations returns the operations for a given table
func getTableOperations(cfg *config.Config, tableName string) []string {
	for _, table := range cfg.Replication.Tables {
		if table.Name == tableName {
			if len(table.Operations) > 0 {
				return table.Operations
			}
			break
		}
	}
	return cfg.Replication.DefaultOps
}

// getTableTopic returns the topic for a given table
func getTableTopic(cfg *config.Config, tableName string) string {
	for _, table := range cfg.Replication.Tables {
		if table.Name == tableName {
			return table.Topic
		}
	}
	return ""
}

// isTableAllowed checks if the table should be processed
func isTableAllowed(cfg *config.Config, tableFullName string) bool {
	// If no tables configured, allow all
	if len(cfg.Replication.Tables) == 0 {
		return true
	}

	// Check if table is in the configured list
	for _, table := range cfg.Replication.Tables {
		if table.Name == tableFullName {
			return true
		}
	}

	return false
}

// getTableNames extracts table names from TableConfig slice
func getTableNames(tables []config.TableConfig) []string {
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Name
	}
	return names
}
