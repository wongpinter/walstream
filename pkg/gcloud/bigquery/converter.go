// Package bigquery provides functionality for interacting with Google BigQuery,
// including schema conversion from PostgreSQL and table management.
package bigquery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/lib/pq"
)

// SchemaConversionError represents errors during schema conversion process.
type SchemaConversionError struct {
	Table string
	Err   error
}

// SchemaTable represents a BigQuery table schema
type SchemaTable struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
}

func (e *SchemaConversionError) Error() string {
	return fmt.Sprintf("schema conversion error for table %s: %v", e.Table, e.Err)
}

// Config holds the database connection configuration.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
}

// Converter defines the interface for schema conversion operations.
type Converter interface {
	ListTables(ctx context.Context) ([]string, error)
	ConvertAndStoreTables(ctx context.Context, tables []SchemaTable) error
	LoadSchema(table string) ([]AvroField, error)
	// ValidateSchema(ctx context.Context, table string) error
	Close() error
}

// SchemaConverter implements the Converter interface.
type SchemaConverter struct {
	db          *sql.DB
	schemaDir   string
	schemaFiles []string
}

// AvroField represents a field in the AVRO schema.
type AvroField struct {
	Mode   string      `json:"mode"`
	Name   string      `json:"name"`
	Type   string      `json:"type"`
	Fields []AvroField `json:"fields,omitempty"`
}

// TopicSchema represents a Pub/Sub topic schema.
type TopicSchema struct {
	Name   string      `json:"name"`
	Type   string      `json:"type"`
	Fields []AvroField `json:"fields"`
}

// NewConverter creates a new instance of Converter.
func NewConverter(cfg Config) (Converter, error) {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	tempDir, err := os.MkdirTemp("", "schema_*")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create temporary directory: %w", err)
	}

	return &SchemaConverter{
		db:          db,
		schemaDir:   tempDir,
		schemaFiles: make([]string, 0),
	}, nil
}

// Close closes the database connection and cleans up temporary files.
func (c *SchemaConverter) Close() error {
	dbErr := c.db.Close()
	cleanupErr := c.cleanup()

	if dbErr != nil {
		return fmt.Errorf("failed to close database connection: %w", dbErr)
	}
	if cleanupErr != nil {
		return fmt.Errorf("failed to cleanup schema files: %w", cleanupErr)
	}

	return nil
}

// cleanup removes all temporary schema files and directory.
func (c *SchemaConverter) cleanup() error {
	if c.schemaDir != "" {
		if err := os.RemoveAll(c.schemaDir); err != nil {
			return fmt.Errorf("failed to remove schema directory: %w", err)
		}
		c.schemaDir = ""
		c.schemaFiles = nil
	}
	return nil
}

// ListTables returns a list of all tables in the database.
func (c *SchemaConverter) ListTables(ctx context.Context) ([]string, error) {
	query := `
		SELECT table_name 
		FROM information_schema.tables 
		WHERE table_schema = 'public'
		ORDER BY table_name;
	`

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

// ConvertAndStoreTables converts the specified tables to BigQuery schema and stores them as JSON files.
func (c *SchemaConverter) ConvertAndStoreTables(ctx context.Context, tables []SchemaTable) error {
	for _, table := range tables {
		schema, err := c.convertTable(ctx, table.Name, table.Columns)
		if err != nil {
			return &SchemaConversionError{Table: table.Name, Err: err}
		}

		if err := c.storeSchema(table.Name, schema); err != nil {
			return &SchemaConversionError{Table: table.Name, Err: err}
		}
	}
	return nil
}

// getSchemaFilePath returns the path to the schema file for a given table.
func (c *SchemaConverter) getSchemaFilePath(table string) string {
	return filepath.Join(c.schemaDir, fmt.Sprintf("%s_schema.json", table))
}

// storeSchema stores the converted schema as a JSON file.
func (c *SchemaConverter) storeSchema(table string, schema []AvroField) error {
	filePath := c.getSchemaFilePath(table)

	schemaJSON, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}

	if err := os.WriteFile(filePath, schemaJSON, 0644); err != nil {
		return fmt.Errorf("failed to write schema file: %w", err)
	}

	c.schemaFiles = append(c.schemaFiles, filePath)
	return nil
}

// LoadSchema loads the schema from the temporary JSON file.
func (c *SchemaConverter) LoadSchema(table string) ([]AvroField, error) {
	filePath := c.getSchemaFilePath(table)

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	var schema []AvroField
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	return schema, nil
}

// ValidateSchema validates the schema conversion by comparing source and destination schemas.
// func (c *SchemaConverter) ValidateSchema(ctx context.Context, table string) error {
// 	sourceSchema, err := c.convertTable(ctx, table)
// 	if err != nil {
// 		return fmt.Errorf("failed to get source schema: %w", err)
// 	}

// 	destSchema, err := c.LoadSchema(table)
// 	if err != nil {
// 		return fmt.Errorf("failed to load destination schema: %w", err)
// 	}

// 	if !c.compareSchemas(sourceSchema, destSchema) {
// 		return fmt.Errorf("schema mismatch for table %s", table)
// 	}

// 	return nil
// }

// compareSchemas compares two schemas for equality.
// func (c *SchemaConverter) compareSchemas(source, dest []AvroField) bool {
// 	if len(source) != len(dest) {
// 		return false
// 	}

// 	for i, srcField := range source {
// 		destField := dest[i]
// 		if srcField.Name != destField.Name ||
// 			srcField.Type != destField.Type ||
// 			srcField.Mode != destField.Mode {
// 			return false
// 		}

// 		if len(srcField.Fields) != len(destField.Fields) {
// 			return false
// 		}

// 		if len(srcField.Fields) > 0 {
// 			if !c.compareSchemas(srcField.Fields, destField.Fields) {
// 				return false
// 			}
// 		}
// 	}

// 	return true
// }

// convertTable converts a single table to BigQuery schema.
func (c *SchemaConverter) convertTable(ctx context.Context, tableName string, columns []string) ([]AvroField, error) {
	query := `
		SELECT 
			column_name, 
			data_type,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			is_nullable,
			column_default,
			CASE 
				WHEN column_default LIKE 'nextval%' THEN true 
				ELSE false 
			END as is_serial
		FROM information_schema.columns 
		WHERE table_schema = 'public' 
		AND table_name = $1
		ORDER BY ordinal_position;
	`

	rows, err := c.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var fields []AvroField
	for rows.Next() {

		var (
			columnName       string
			dataType         string
			charMaxLength    sql.NullInt64
			numericPrecision sql.NullInt64
			numericScale     sql.NullInt64
			isNullable       string
			columnDefault    sql.NullString
			isSerial         bool
		)

		if err := rows.Scan(
			&columnName,
			&dataType,
			&charMaxLength,
			&numericPrecision,
			&numericScale,
			&isNullable,
			&columnDefault,
			&isSerial,
		); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		if len(columns) > 0 && !stringInSlice(columnName, columns) {
			continue
		}

		field := AvroField{
			Name: columnName,
		}

		if isSerial || (columnName == "id" && strings.Contains(strings.ToLower(dataType), "int")) {
			field.Mode = "REQUIRED"
		} else if isNullable == "NO" {
			field.Mode = "REQUIRED"
		} else {
			field.Mode = "NULLABLE"
		}

		switch strings.ToLower(dataType) {
		case "numeric", "double precision":
			field.Type = "NUMERIC"
		case "integer", "smallint", "bigint", "int":
			field.Type = "INTEGER"

			if isSerial {
				field.Type = "BIGINT"
			}
		case "real", "decimal":
			field.Type = "FLOAT"
		case "character varying", "text", "character":
			field.Type = "STRING"
		case "boolean":
			field.Type = "BOOLEAN"
		case "timestamp without time zone", "timestamp with time zone":
			field.Type = "TIMESTAMP"
		case "date":
			field.Type = "DATE"
		case "time without time zone", "time with time zone":
			field.Type = "TIME"
		case "json", "jsonb":
			field.Type = "STRING"
		default:
			field.Type = "STRING"
		}

		fields = append(fields, field)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	// Add _op field
	fields = append(fields, AvroField{
		Name: "__op",
		Type: "STRING",
		Mode: "NULLABLE",
	})

	// Add __deleted field for CDC
	fields = append(fields, AvroField{
		Name: "__deleted",
		Type: "STRING",
		Mode: "NULLABLE",
	})

	// add __timestamp field
	fields = append(fields, AvroField{
		Name: "__timestamp",
		Type: "DATETIME",
		Mode: "REQUIRED",
	})

	return fields, nil
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
