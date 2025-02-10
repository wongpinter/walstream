package model

import (
	"encoding/json"
	"time"
)

// DataType represents a PostgreSQL data type
type DataType struct {
	Name      string // e.g., "integer", "character varying"
	TypeOid   uint32 // PostgreSQL type OID
	ArrayType bool   // Whether this is an array type
	Length    int    // Length for varchar(n), etc.
	Scale     int    // Decimal scale
	Precision int    // Decimal precision
}

// ColumnDefinition represents a column in a table
type ColumnDefinition struct {
	Name       string   `json:"name"`
	Type       DataType `json:"type"`
	Order      int      `json:"order"`      // Column position in table
	Optional   bool     `json:"optional"`    // Whether column is nullable
	IsKey      bool     `json:"is_key"`     // Whether column is part of primary key
	IsGenerated bool    `json:"is_generated"` // Whether column is generated
	Default    *string  `json:"default,omitempty"` // Default value expression
}

// TableSchema represents the schema of a table
type TableSchema struct {
	Columns     []ColumnDefinition `json:"columns"`
	PrimaryKey  []string          `json:"primary_key"`  // Names of primary key columns
	UniqueKeys  [][]string        `json:"unique_keys"`  // Sets of unique key columns
	ForeignKeys []ForeignKey      `json:"foreign_keys"` // Foreign key constraints
}

// ForeignKey represents a foreign key constraint
type ForeignKey struct {
	Columns          []string `json:"columns"`           // Local columns
	ReferencedSchema string   `json:"referenced_schema"` // Referenced schema name
	ReferencedTable  string   `json:"referenced_table"`  // Referenced table name
	ReferencedColumns []string `json:"referenced_columns"` // Referenced columns
	OnDelete         string   `json:"on_delete"`         // ON DELETE action
	OnUpdate         string   `json:"on_update"`         // ON UPDATE action
}

// Message represents a change event from the WAL
type Message struct {
	// Event metadata
	ID            string    `json:"id"`             // Unique event ID
	TransactionID string    `json:"transaction_id"` // PostgreSQL transaction ID
	Operation     string    `json:"operation"`      // INSERT, UPDATE, DELETE, CREATE, ALTER, TRUNCATE
	LSN           uint64    `json:"lsn"`           // Log Sequence Number
	Timestamp     time.Time `json:"timestamp"`      // Timestamp of the change

	// Table information
	Schema string      `json:"schema"` // Schema name
	Table  string      `json:"table"`  // Table name
	Object TableSchema `json:"object"` // Full table schema

	// Change data
	Before map[string]interface{} `json:"before,omitempty"` // Old values (for UPDATE/DELETE)
	After  map[string]interface{} `json:"after,omitempty"`  // New values (for INSERT/UPDATE)

	// Additional metadata
	EventType    string                 `json:"event_type"`              // DML or DDL
	Tags         map[string]string      `json:"tags,omitempty"`         // Custom tags
	Metadata     map[string]interface{} `json:"metadata,omitempty"`     // Additional metadata
	DebugContext map[string]interface{} `json:"debug_context,omitempty"` // Debug information
}

// MarshalJSON implements custom JSON marshaling to handle PostgreSQL-specific types
func (m *Message) MarshalJSON() ([]byte, error) {
	type Alias Message
	return json.Marshal(&struct {
		*Alias
		Timestamp string `json:"timestamp"`
	}{
		Alias:     (*Alias)(m),
		Timestamp: m.Timestamp.Format(time.RFC3339Nano),
	})
}

// UnmarshalJSON implements custom JSON unmarshaling to handle PostgreSQL-specific types
func (m *Message) UnmarshalJSON(data []byte) error {
	type Alias Message
	aux := &struct {
		*Alias
		Timestamp string `json:"timestamp"`
	}{
		Alias: (*Alias)(m),
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	var err error
	m.Timestamp, err = time.Parse(time.RFC3339Nano, aux.Timestamp)
	return err
}
