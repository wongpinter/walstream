package streamer

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/broker"
	"repo.nusatek.id/sugeng/walstreamer/broker/inmemory"
	"repo.nusatek.id/sugeng/walstreamer/broker/pubsub"
	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/lsn"
	"repo.nusatek.id/sugeng/walstreamer/model"
	"repo.nusatek.id/sugeng/walstreamer/replication"
	"repo.nusatek.id/sugeng/walstreamer/sync"
	"repo.nusatek.id/sugeng/walstreamer/wal"
)

// Streamer handles the WAL streaming process
type Streamer struct {
	cfg           *config.Config
	logger        *zerolog.Logger
	messageBroker broker.Broker
	replManager   *replication.Manager
	reader        *wal.Reader
	storage       lsn.Storage
}

// New creates a new streamer instance
func New(cfg *config.Config, logger *zerolog.Logger) (*Streamer, error) {
	// Initialize broker
	messageBroker, err := createBroker(cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create broker: %w", err)
	}

	// Create LSN storage
	storage, err := lsn.NewFileStorage(cfg.LSN.Path, time.Duration(cfg.LSN.PersistInterval)*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to create LSN storage: %w", err)
	}

	// Create replication manager
	replManager := replication.NewManager(replication.Config{
		PublicationName: cfg.Replication.Publication,
		SlotName:        cfg.Replication.Slot,
		IncludedTables:  cfg.GetIncludedTables(),
	})

	return &Streamer{
		cfg:           cfg,
		logger:        logger,
		messageBroker: messageBroker,
		replManager:   replManager,
		storage:       storage,
	}, nil
}

// Start begins the streaming process
func (s *Streamer) Start(ctx context.Context) error {
	// Connect to database
	if err := s.replManager.Connect(ctx, s.cfg.Database.GetDSN()); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// Setup replication
	if err := s.replManager.Setup(ctx); err != nil {
		return fmt.Errorf("failed to setup replication: %w", err)
	}

	// Check if LSN file exists and handle initial sync
	if s.cfg.Replication.InitialSync {
		lsnExists, err := s.storage.Exists()
		if err != nil {
			return fmt.Errorf("failed to check LSN file: %w", err)
		}

		if !lsnExists {
			s.logger.Info().Msg("LSN file not found, starting initial sync")
			syncer := sync.NewInitialSyncer(s.cfg, s.messageBroker, s.logger)
			if err := syncer.Start(ctx); err != nil {
				return fmt.Errorf("failed to perform initial sync: %w", err)
			}
			// Set initial LSN after sync completes
			if err := s.storage.Set(s.cfg.Replication.Publication, 0); err != nil {
				return fmt.Errorf("failed to set initial LSN: %w", err)
			}
			s.logger.Info().Msg("Initial sync completed")
		}
	}

	// Create WAL reader
	reader, err := s.createWALReader(ctx)
	if err != nil {
		return fmt.Errorf("failed to create WAL reader: %w", err)
	}
	s.reader = reader

	// Start reading WAL
	if err := s.reader.Start(ctx); err != nil {
		return fmt.Errorf("WAL reader failed: %w", err)
	}

	return nil
}

// Close cleans up resources
func (s *Streamer) Close() error {
	var errs []error

	if s.storage != nil {
		if err := s.storage.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close LSN storage: %w", err))
		}
	}

	if s.replManager != nil {
		if err := s.replManager.Close(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("failed to close replication manager: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing: %v", errs)
	}
	return nil
}

func createBroker(cfg *config.Config, logger *zerolog.Logger) (broker.Broker, error) {
	brokerConfig := broker.BrokerConfig{
		BufferSize: 1000,
		BatchConfig: broker.BatchConfig{
			Size:          100,
			Workers:       2,
			FlushInterval: 5 * time.Second,
		},
		ShutdownTimeout:  30 * time.Second,
		HeartbeatTimeout: 5 * time.Second,
	}

	switch cfg.Broker.Type {
	case "pubsub":
		pubsubConfig := pubsub.Config{
			ProjectID:       cfg.Broker.PubSub.ProjectID,
			TopicPrefix:     cfg.Broker.PubSub.TopicPrefix,
			CredentialsFile: cfg.Broker.PubSub.CredentialsFile,
			AutoCreateTopic: cfg.Broker.PubSub.AutoCreateTopic,
			Logger:          logger,
		}
		return pubsub.NewBroker(pubsubConfig)
	default:
		return inmemory.NewInMemoryBroker(brokerConfig), nil
	}
}

func (s *Streamer) createWALReader(_ context.Context) (*wal.Reader, error) {
	walConfig := wal.Config{
		ConnString:            s.cfg.Database.GetReplicationDSN(),
		PublicationName:       s.cfg.Replication.Publication,
		SlotName:              s.cfg.Replication.Slot,
		StandbyTimeout:        time.Duration(s.cfg.Replication.StandbyTimeout) * time.Second,
		Logger:                s.logger,
		LSNStorage:            s.storage,
		MaxReconnectAttempts:  s.cfg.Replication.Reconnect.MaxAttempts,
		ReconnectInitialDelay: time.Duration(s.cfg.Replication.Reconnect.InitialDelay) * time.Second,
		ReconnectMaxDelay:     time.Duration(s.cfg.Replication.Reconnect.MaxDelay) * time.Second,
	}

	return wal.NewReader(walConfig, s.createMessageHandler())
}

func (s *Streamer) createMessageHandler() func(msg *model.Message) error {
	return func(msg *model.Message) error {
		// Get table name
		tableFullName := msg.Table
		if tableFullName == "" {
			s.logger.Debug().Msg("skipping message without table name")
			return nil
		}

		s.logger.Debug().
			Str("table", tableFullName).
			Strs("patterns", s.cfg.GetIncludedTables()).
			Msg("Checking table patterns")

		// Check if table is allowed
		if !s.isTableAllowed(tableFullName) {
			s.logger.Debug().
				Str("table", tableFullName).
				Msg("Table not in configured list")
			return nil
		}

		// Get operations for the table
		operations := s.getTableOperations(tableFullName)
		s.logger.Debug().
			Str("table", tableFullName).
			Strs("operations", operations).
			Msg("Table operations")

		// Get topic for the table if specified
		topic := s.getTableTopic(tableFullName)
		if topic != "" {
			s.logger.Debug().
				Str("table", tableFullName).
				Str("topic", topic).
				Msg("Using custom topic for table")
		}

		// filter just publish columns defined from table
		msg.Before = s.filterColumns(tableFullName, msg.Before)
		msg.After = s.filterColumns(tableFullName, msg.After)

		// Check if operation is allowed
		operationAllowed := false
		if len(operations) == 0 {
			operationAllowed = true
		} else {
			for _, op := range operations {
				if op == msg.Operation {
					operationAllowed = true
					break
				}
			}
		}
		if !operationAllowed {
			s.logger.Debug().
				Str("operation", msg.Operation).
				Str("table", tableFullName).
				Strs("allowed_ops", operations).
				Msg("Operation not allowed for table")
			return nil
		}

		// Print message details for debugging
		s.logger.Debug().
			Str("operation", msg.Operation).
			Str("schema", msg.Schema).
			Str("table", msg.Table).
			Interface("before", msg.Before).
			Interface("after", msg.After).
			Msg("Processing message")

		s.messageBroker.AddTransformer(func(m *model.Message) (interface{}, error) {
			return m.ToFormat(model.RecordFormat), nil
		})

		if err := s.messageBroker.Publish(context.Background(), msg); err != nil {
			return fmt.Errorf("failed to publish message: %w", err)
		}

		s.logger.Info().Str("operation", msg.Operation).Str("table", msg.Table).Msg("Published message")

		// Print broker metrics for debugging
		metrics := s.messageBroker.Metrics()
		s.logger.Info().
			Int64("messages_published", metrics.MessagesPublished).
			Int64("messages_failed", metrics.MessagesFailed).
			Int("buffer_size", metrics.BufferSize).
			Msg("Broker metrics")

		return nil
	}
}

func (s *Streamer) isTableAllowed(tableFullName string) bool {
	for _, table := range s.cfg.Replication.GetTableNames() {
		if table == tableFullName {
			return true
		}
	}
	return false
}

func (s *Streamer) getTableOperations(tableName string) []string {
	for _, table := range s.cfg.Replication.GetTableNames() {
		if table == tableName {
			operations, _ := s.cfg.Replication.GetTableOperations(table)
			return operations
		}
	}
	return nil
}

func (s *Streamer) getTableTopic(tableName string) string {
	for _, table := range s.cfg.Replication.GetTableNames() {
		tableConf, err := s.cfg.Replication.GetTableConfig(table)
		if err != nil {
			continue
		}

		if table == tableName && tableConf.Topic != "" {
			return tableConf.Topic
		}
	}
	return ""
}

func (s *Streamer) filterColumns(tableFullName string, columns map[string]interface{}) map[string]interface{} {
	filteredColumns := make(map[string]interface{})
	for column, value := range columns {
		if s.isColumnAllowed(tableFullName, column) {
			filteredColumns[column] = value
		}
	}
	return filteredColumns
}

func (s *Streamer) isColumnAllowed(tableFullName string, column string) bool {
	for _, table := range s.cfg.Replication.GetTableNames() {
		if table == tableFullName {
			columns, _ := s.cfg.Replication.GetColumnsForTable(table)

			for _, allowedColumn := range columns {
				if column == allowedColumn {
					return true
				}
			}
			return false
		}
	}
	return false
}
