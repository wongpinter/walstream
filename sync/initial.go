package sync

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/broker"
	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/decoder/pgoutput"
	"repo.nusatek.id/sugeng/walstreamer/model"
)

// InitialSyncer handles the initial synchronization of existing data
type InitialSyncer struct {
	cfg    *config.Config
	broker broker.Broker
	logger *zerolog.Logger
	conn   *pgx.Conn
}

type TableSync struct {
	Name    string
	Columns []string
}

// NewInitialSyncer creates a new InitialSyncer
func NewInitialSyncer(cfg *config.Config, broker broker.Broker, logger *zerolog.Logger) *InitialSyncer {
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
	tables, err := s.getTables()
	if err != nil {
		return fmt.Errorf("failed to get target tables: %w", err)
	}

	// Process each table
	for _, table := range tables {
		if err := s.syncTable(ctx, table.Name, table.Columns); err != nil {
			return fmt.Errorf("failed to sync table %s: %w", table, err)
		}
	}

	return nil
}

// getTables returns a list of tables to sync
func (s *InitialSyncer) getTables() ([]TableSync, error) {
	var tables []TableSync
	seen := make(map[string]bool)

	// Add tables from configuration
	for _, table := range s.cfg.Replication.GetTableNames() {
		if table != "" && !seen[table] && !strings.HasPrefix(table, "!") {
			columns, err := s.cfg.Replication.GetColumnsForTable(table)
			if err != nil {
				return nil, fmt.Errorf("failed to get columns for table %s: %w", table, err)
			}

			tables = append(tables, TableSync{Name: table, Columns: columns})
			seen[table] = true
		}
	}

	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables configured for initial sync")
	}

	return tables, nil
}

// syncTable synchronizes a single table
func (s *InitialSyncer) syncTable(ctx context.Context, tableName string, columns []string) error {
	s.logger.Info().Str("table", tableName).Msg("starting initial sync")

	// Get table schema
	schema, err := s.getTableSchema()
	if err != nil {
		return fmt.Errorf("failed to get table schema: %w", err)
	}

	query := fmt.Sprintf("SELECT * FROM %s", tableName)

	// Build query
	if len(columns) > 0 {
		columnsStr := strings.Join(columns, ", ")
		query = fmt.Sprintf("SELECT %s FROM %s", columnsStr, tableName)
	}

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
			s.parseValur(field, values, i, data)
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

// parseValur parses a value based on its data type
func (*InitialSyncer) parseValur(field pgconn.FieldDescription, values []any, i int, data map[string]interface{}) {
	switch field.DataTypeOID {
	case pgoutput.OIDDate:
		v, ok := values[i].(time.Time)
		if ok {
			data[string(field.Name)] = v.Format("2006-01-02")
		} else {
			data[string(field.Name)] = fmt.Sprintf("%v", values[i])
		}
	case pgoutput.OIDTimestamp:
		v, ok := values[i].(time.Time)
		if ok {
			data[string(field.Name)] = v.Format("2006-01-02 15:04:05.999999")
		} else {
			data[string(field.Name)] = fmt.Sprintf("%v", values[i])
		}
	default:
		data[string(field.Name)] = values[i]
	}
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

		s.broker.AddTransformer(func(m *model.Message) (interface{}, error) {
			return m.ToFormat(model.RecordFormat), nil
		})

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
