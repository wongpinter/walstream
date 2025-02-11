// Package replication provides functionality for managing PostgreSQL logical replication
// setup, including publication and replication slot management.
package replication

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Config holds the configuration for replication setup.
// It defines how the replication should be configured, including which tables
// to replicate and how the replication slot should be created.
type Config struct {
	// PublicationName is the name of the PostgreSQL publication to create or use
	PublicationName string

	// SlotName is the name of the replication slot to create or use
	SlotName string

	// IncludedTables is a list of tables to include in the publication.
	// If empty, all tables will be included.
	// Format: schema.table (e.g., "public.users")
	IncludedTables []string

	// ExcludedTables is a list of tables to exclude from the publication.
	// This takes precedence over IncludedTables.
	// Format: schema.table (e.g., "public.users")
	ExcludedTables []string

	// TempSlot indicates whether to create a temporary replication slot.
	// Temporary slots are automatically dropped when the session ends.
	TempSlot bool
}

// Manager handles replication setup and management for PostgreSQL logical replication.
type Manager struct {
	config Config
	conn   *pgx.Conn
}

// NewManager creates a new replication manager with the given configuration.
// The manager must be connected using Connect() before it can be used.
func NewManager(config Config) *Manager {
	return &Manager{
		config: config,
	}
}

// Connect establishes a connection to PostgreSQL using the provided connection string.
// The connection string should be in the format:
// "postgres://user:password@host:port/dbname?sslmode=disable"
func (m *Manager) Connect(ctx context.Context, connString string) error {
	// Remove replication=database from connection string for setup connection
	connStr := strings.ReplaceAll(connString, "replication=database", "")
	var err error
	m.conn, err = pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	return nil
}

// Close closes the PostgreSQL connection.
// It should be called when the manager is no longer needed to free up resources.
func (m *Manager) Close(ctx context.Context) error {
	if m.conn != nil {
		return m.conn.Close(ctx)
	}
	return nil
}

// Setup creates the publication and replication slot if they don't exist.
// If the publication exists, it will be updated with the current table configuration.
// If the replication slot exists, it will be reused.
func (m *Manager) Setup(ctx context.Context) error {
	// Cleanup any existing resources
	// if err := m.Cleanup(ctx); err != nil {
	// 	return fmt.Errorf("failed to cleanup existing resources: %w", err)
	// }

	if err := m.createPublication(ctx); err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	if err := m.createReplicationSlot(ctx); err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	return nil
}

// createPublication creates a new publication if it doesn't exist
func (m *Manager) createPublication(ctx context.Context) error {
	// Check if publication exists
	var exists bool
	err := m.conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
		m.config.PublicationName,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check publication existence: %w", err)
	}

	if exists {
		// Update publication if it exists
		return m.updatePublication(ctx)
	}

	// Create new publication
	var query string
	if len(m.config.IncludedTables) == 0 {
		// Use proper identifier quoting for publication name
		pubName := pgx.Identifier{m.config.PublicationName}.Sanitize()
		query = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pubName)
	} else {
		tables := make([]string, 0, len(m.config.IncludedTables))
		for _, table := range m.config.IncludedTables {
			if !m.isExcluded(table) {
				tables = append(tables, pgx.Identifier{table}.Sanitize())
			}
		}
		// Use proper identifier quoting for publication name and table names
		pubName := pgx.Identifier{m.config.PublicationName}.Sanitize()
		query = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
			pubName,
			strings.Join(tables, ", "))
	}

	_, err = m.conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	return nil
}

// updatePublication updates an existing publication with current table list
func (m *Manager) updatePublication(ctx context.Context) error {
	// Check if publication is FOR ALL TABLES
	var isForAllTables bool
	err := m.conn.QueryRow(ctx,
		"SELECT puballtables FROM pg_publication WHERE pubname = $1",
		m.config.PublicationName,
	).Scan(&isForAllTables)
	if err != nil {
		return fmt.Errorf("failed to check publication type: %w", err)
	}

	// If current config wants all tables and publication is already FOR ALL TABLES,
	// or if current config has specific tables and publication is not FOR ALL TABLES,
	// then no change is needed
	configWantsAllTables := len(m.config.IncludedTables) == 0
	if configWantsAllTables == isForAllTables {
		return nil
	}

	// Drop the existing publication
	dropQuery := fmt.Sprintf("DROP PUBLICATION %s",
		pgx.Identifier{m.config.PublicationName}.Sanitize())
	_, err = m.conn.Exec(ctx, dropQuery)
	if err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}

	// Create new publication with desired configuration
	var createQuery string
	pubName := pgx.Identifier{m.config.PublicationName}.Sanitize()
	if configWantsAllTables {
		createQuery = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", pubName)
	} else {
		tables := make([]string, 0, len(m.config.IncludedTables))
		for _, table := range m.config.IncludedTables {
			if !m.isExcluded(table) {
				tables = append(tables, pgx.Identifier{table}.Sanitize())
			}
		}
		createQuery = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s",
			pubName,
			strings.Join(tables, ", "))
	}

	_, err = m.conn.Exec(ctx, createQuery)
	if err != nil {
		return fmt.Errorf("failed to recreate publication: %w", err)
	}

	return nil
}

// createReplicationSlot creates a new replication slot if it doesn't exist
func (m *Manager) createReplicationSlot(ctx context.Context) error {
	// Check if slot exists
	var exists bool
	err := m.conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		m.config.SlotName,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check replication slot existence: %w", err)
	}

	if exists {
		return nil // Slot already exists
	}

	// Create new slot
	query := "SELECT pg_create_logical_replication_slot($1, 'pgoutput'"
	args := []interface{}{m.config.SlotName}

	if m.config.TempSlot {
		query += ", true, true"
	} else {
		query += ", false, false"
	}
	query += ")"

	_, err = m.conn.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	return nil
}

// CleanupPublication drops the existing publication if it exists
func (m *Manager) CleanupPublication(ctx context.Context) error {
	// Check if publication exists
	var exists bool
	err := m.conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)",
		m.config.PublicationName,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check publication existence: %w", err)
	}

	if exists {
		// Fix: Changed $1 to %s for proper SQL string formatting
		sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s",
			pgx.Identifier{m.config.PublicationName}.Sanitize())
		_, err := m.conn.Exec(ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to drop publication %s: %w", m.config.PublicationName, err)
		}
	}
	return nil
}

// CleanupReplicationSlot drops the existing replication slot if it exists
func (m *Manager) CleanupReplicationSlot(ctx context.Context) error {
	// Check if slot exists
	var exists bool
	err := m.conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1 AND slot_type = 'logical')",
		m.config.SlotName,
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check replication slot existence: %w", err)
	}

	if exists {
		// First check if slot is active
		var active bool
		err := m.conn.QueryRow(ctx,
			"SELECT active FROM pg_replication_slots WHERE slot_name = $1",
			m.config.SlotName,
		).Scan(&active)
		if err != nil {
			return fmt.Errorf("failed to check if replication slot is active: %w", err)
		}

		if active {
			// If slot is active, try to terminate any existing connections
			_, err := m.conn.Exec(ctx,
				"SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL",
				m.config.SlotName,
			)
			if err != nil {
				return fmt.Errorf("failed to terminate active connections for slot %s: %w", m.config.SlotName, err)
			}
		}

		_, err = m.conn.Exec(ctx,
			"SELECT pg_drop_replication_slot($1)",
			m.config.SlotName,
		)
		if err != nil {
			return fmt.Errorf("failed to drop replication slot %s: %w", m.config.SlotName, err)
		}
	}
	return nil
}

// Cleanup drops both the publication and replication slot if they exist.
// It will attempt to clean up both resources even if one fails, collecting all errors.
func (m *Manager) Cleanup(ctx context.Context) error {
	var errs []error

	// Cleanup publication
	if err := m.CleanupPublication(ctx); err != nil {
		errs = append(errs, fmt.Errorf("failed to cleanup publication: %w", err))
	}

	// Cleanup replication slot
	if err := m.CleanupReplicationSlot(ctx); err != nil {
		errs = append(errs, fmt.Errorf("failed to cleanup replication slot: %w", err))
	}

	// If we have any errors, combine them
	if len(errs) > 0 {
		var errStr strings.Builder
		for i, err := range errs {
			if i > 0 {
				errStr.WriteString("; ")
			}
			errStr.WriteString(err.Error())
		}
		return fmt.Errorf("cleanup errors: %s", errStr.String())
	}

	return nil
}

// isExcluded checks if a table is in the excluded list
func (m *Manager) isExcluded(table string) bool {
	for _, excluded := range m.config.ExcludedTables {
		if table == excluded {
			return true
		}
	}
	return false
}
