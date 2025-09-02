package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/wongpinter/walstreamer/logging"
)

// Manager handles BigQuery operations
type Manager interface {
	CreateDataset(ctx context.Context, datasetID string) error
	CreateTable(ctx context.Context, datasetID, tableID string, schema bigquery.Schema) error
	CreateTableFromSchemas(ctx context.Context, datasetID string, schemas map[string][]AvroField) error
	TableExists(ctx context.Context, datasetID, tableID string) (bool, error)
	Client() *bigquery.Client
	Close() error
}

// BigQueryManager implements Manager interface
type BigQueryManager struct {
	client   *bigquery.Client
	logger   *logging.Logger
	location string
}

// NewManager creates a new BigQuery manager
func NewManager(ctx context.Context, projectID string, credentials string, location string, logger *logging.Logger) (Manager, error) {
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return &BigQueryManager{
		client:   client,
		logger:   logger.WithComponent("bigquery"),
		location: location,
	}, nil
}

// CreateDataset creates a new BigQuery dataset if it doesn't exist
func (m *BigQueryManager) CreateDataset(ctx context.Context, datasetID string) error {
	dataset := m.client.Dataset(datasetID)
	metadata, err := dataset.Metadata(ctx)

	if err == nil && metadata != nil {
		m.logger.Debug().Msgf("Dataset %s already exists", datasetID)
		return nil
	}

	// Check if error is "not found"
	if e, ok := err.(*googleapi.Error); !ok || e.Code != 404 {
		return fmt.Errorf("failed to check dataset existence: %w", err)
	}

	// Create dataset since it doesn't exist
	m.logger.Info().Msgf("Creating dataset %s", datasetID)
	if err := dataset.Create(ctx, &bigquery.DatasetMetadata{
		Location: m.location,
	}); err != nil {
		return fmt.Errorf("failed to create dataset %s: %w", datasetID, err)
	}

	m.logger.Info().Msgf("Created dataset %s", datasetID)
	return nil
}

// CreateTable creates a new BigQuery table if it doesn't exist
func (m *BigQueryManager) CreateTable(ctx context.Context, datasetID, tableID string, schema bigquery.Schema) error {
	table := m.client.Dataset(datasetID).Table(tableID)
	exists, err := m.TableExists(ctx, datasetID, tableID)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		m.logger.Debug().Msgf("Table %s.%s already exists", datasetID, tableID)
		return nil
	}

	// Create schema from table schema
	if err := table.Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
	}); err != nil {
		return fmt.Errorf("failed to create table %s.%s: %w", datasetID, tableID, err)
	}

	m.logger.Info().Msgf("Created table %s.%s", datasetID, tableID)
	return nil
}

func (m *BigQueryManager) CreateTableFromSchemas(ctx context.Context, datasetID string, schemas map[string][]AvroField) error {
	for table, schema := range schemas {
		tableSchema := createCDCSchema(schema)
		if err := m.CreateTable(ctx, datasetID, table, tableSchema); err != nil {
			return err
		}
	}

	return nil
}

// TableExists checks if a table exists in BigQuery
func (m *BigQueryManager) TableExists(ctx context.Context, datasetID, tableID string) (bool, error) {
	table := m.client.Dataset(datasetID).Table(tableID)
	_, err := table.Metadata(ctx)
	if err == nil {
		return true, nil
	}

	// Check if error is "not found"
	if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
		return false, nil
	}

	return false, fmt.Errorf("failed to check table existence: %w", err)
}

// Client returns the underlying BigQuery client
func (m *BigQueryManager) Client() *bigquery.Client {
	return m.client
}

// Close closes the BigQuery client
func (m *BigQueryManager) Close() error {
	if err := m.client.Close(); err != nil {
		return fmt.Errorf("failed to close BigQuery client: %w", err)
	}
	return nil
}

func createCDCSchema(tableSchema []AvroField) bigquery.Schema {
	// Convert schema fields
	recordSchema := make(bigquery.Schema, 0, len(tableSchema))
	for _, field := range tableSchema {
		recordSchema = append(recordSchema, convertAvroField(field))
	}

	return recordSchema
}

// convertAvroField converts an AvroField to a BigQuery FieldSchema
func convertAvroField(field AvroField) *bigquery.FieldSchema {
	bqField := &bigquery.FieldSchema{
		Name:     field.Name,
		Type:     convertAvroFieldType(field.Type),
		Required: field.Mode == "REQUIRED",
	}

	if field.Type == "RECORD" && len(field.Fields) > 0 {
		var subFields bigquery.Schema
		for _, subField := range field.Fields {
			subFields = append(subFields, convertAvroField(subField))
		}
		bqField.Schema = subFields
	}

	return bqField
}

// convertAvroFieldType converts AvroField type to BigQuery FieldType
func convertAvroFieldType(avroType string) bigquery.FieldType {
	switch avroType {
	case "NUMERIC":
		return bigquery.NumericFieldType
	case "BIGINT":
		return bigquery.BigNumericFieldType
	case "INTEGER":
		return bigquery.IntegerFieldType
	case "FLOAT":
		return bigquery.FloatFieldType
	case "STRING":
		return bigquery.StringFieldType
	case "BOOLEAN":
		return bigquery.BooleanFieldType
	case "TIMESTAMP":
		return bigquery.TimestampFieldType
	case "DATE":
		return bigquery.DateFieldType
	case "TIME":
		return bigquery.TimeFieldType
	case "RECORD":
		return bigquery.RecordFieldType
	default:
		return bigquery.StringFieldType
	}
}
