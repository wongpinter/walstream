package sync

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/broker"
	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/model"
)

// InitialSyncer handles the initial synchronization of existing data
type InitialSyncer struct {
	cfg    *config.Config
	broker broker.Broker
	logger zerolog.Logger
	conn   *pgx.Conn
}

// NewInitialSyncer creates a new InitialSyncer
func NewInitialSyncer(cfg *config.Config, broker broker.Broker, logger zerolog.Logger) *InitialSyncer {
	return &InitialSyncer{
		cfg:    cfg,
		broker: broker,
		logger: logger,
	}
}

// Start begins the initial sync process
func (s *InitialSyncer) Start(ctx context.Context) error {
	// Connect to database
	conn, err := pgx.Connect(ctx, s.cfg.Database.GetDSN())
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	s.conn = conn
	defer s.conn.Close(ctx)

	// Get list of tables to sync
	tables, err := s.getTargetTables()
	if err != nil {
		return fmt.Errorf("failed to get target tables: %w", err)
	}

	// Process each table
	for _, table := range tables {
		if err := s.syncTable(ctx, table); err != nil {
			return fmt.Errorf("failed to sync table %s: %w", table, err)
		}
	}

	return nil
}

// getTargetTables returns the list of tables to sync
func (s *InitialSyncer) getTargetTables() ([]string, error) {
	var tables []string
	seen := make(map[string]bool)

	// Process table configs first
	for _, tc := range s.cfg.Replication.TableConfigs {
		if !seen[tc.Name] {
			tables = append(tables, tc.Name)
			seen[tc.Name] = true
		}
	}

	// Process legacy table list
	for _, t := range s.cfg.Replication.Tables {
		if t != "" && t[0] != '!' && !seen[t] { // Skip empty, excluded, and already seen tables
			tables = append(tables, t)
			seen[t] = true
		}
	}

	return tables, nil
}

// syncTable synchronizes a single table
func (s *InitialSyncer) syncTable(ctx context.Context, tableName string) error {
	s.logger.Info().Str("table", tableName).Msg("starting initial sync")

	// Get table schema
	schema, err := s.getTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
	}

	// Build query
	query := fmt.Sprintf("SELECT * FROM %s", tableName)

	// Start transaction with repeatable read to ensure consistent snapshot
	tx, err := s.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && err == nil {
			err = fmt.Errorf("failed to rollback transaction: %w", rollbackErr)
		}
	}()

	// Execute query
	rows, err := tx.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to query table: %w", err)
	}
	defer rows.Close()

	// Process rows in batches
	batch := make([]map[string]interface{}, 0, s.cfg.Replication.BatchSize)
	count := 0

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("failed to get row values: %w", err)
		}

		// Create data map
		data := make(map[string]interface{})
		fields := rows.FieldDescriptions()
		for i, field := range fields {
			data[string(field.Name)] = values[i]
		}

		batch = append(batch, data)
		count++

		// Process batch if full
		if len(batch) >= s.cfg.Replication.BatchSize {
			if err := s.publishBatch(ctx, tableName, schema, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Process remaining records
	if len(batch) > 0 {
		if err := s.publishBatch(ctx, tableName, schema, batch); err != nil {
			return err
		}
	}

	s.logger.Info().
		Str("table", tableName).
		Int("count", count).
		Msg("completed initial sync")

	return tx.Commit(ctx)
}

// publishBatch publishes a batch of records
func (s *InitialSyncer) publishBatch(ctx context.Context, tableName string, schema model.TableSchema, batch []map[string]interface{}) error {
	schemaName, tableName := splitTableName(tableName)

	for _, data := range batch {
		msg := &model.Message{
			Operation: "INSERT", // Treat all existing records as inserts
			Schema:    schemaName,
			Table:     tableName,
			After:     data,
			Object:    schema,
			Timestamp: time.Now(),
		}

		if err := s.broker.Publish(ctx, msg); err != nil {
			return fmt.Errorf("failed to publish message: %w", err)
		}
	}

	return nil
}

// getTableSchema retrieves the schema for a table
func (s *InitialSyncer) getTableSchema() (model.TableSchema, error) {
	var schema model.TableSchema
	// Implementation of schema retrieval...
	// This should match the schema retrieval logic used in your WAL reader
	return schema, nil
}

// splitTableName splits a full table name into schema and table parts
func splitTableName(fullName string) (string, string) {
	var schema, table string
	parts := strings.Split(fullName, ".")
	if len(parts) > 1 {
		schema = parts[0]
		table = parts[1]
	} else {
		schema = "public"
		table = parts[0]
	}
	return schema, table
}
