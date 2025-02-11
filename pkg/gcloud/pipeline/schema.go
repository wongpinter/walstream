package pipeline

import (
	"context"
	"fmt"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
	"repo.nusatek.id/sugeng/walstreamer/pkg/gcloud/bigquery"
)

// bigQuerySchemaManager implements SchemaManagerInterface
type bigQuerySchemaManager struct {
	bqManager bigquery.Manager
	config    *config.Config
	logger    *logging.Logger
}

// newBigQuerySchemaManager creates a new bigQuerySchemaManager instance
func newBigQuerySchemaManager(ctx context.Context, cfg *config.Config, logger *logging.Logger, creds string) (SchemaManagerInterface, error) {
	bqMgr, err := bigquery.NewManager(ctx, cfg.Broker.PubSub.ProjectID, creds, cfg.Broker.PubSub.Location, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery manager: %w", err)
	}

	return &bigQuerySchemaManager{
		bqManager: bqMgr,
		config:    cfg,
		logger:    logger,
	}, nil
}

// CreateSchema implements SchemaManagerInterface
func (sm *bigQuerySchemaManager) CreateSchema(ctx context.Context, configName string, tables []string) error {
	converter, err := bigquery.NewConverter(bigquery.Config{
		Host:     sm.config.Database.Host,
		Port:     sm.config.Database.Port,
		User:     sm.config.Database.User,
		Password: sm.config.Database.Password,
		DBName:   sm.config.Database.DBName,
	})
	if err != nil {
		return fmt.Errorf("failed to create schema converter: %w", err)
	}
	defer converter.Close()

	if err := converter.ConvertAndStoreTables(ctx, tables); err != nil {
		return fmt.Errorf("failed to convert and store tables: %w", err)
	}

	tableSchemas := make(map[string][]bigquery.AvroField)
	var validationErrors []error

	for _, table := range tables {
		schema, err := converter.LoadSchema(table)
		if err != nil {
			validationErrors = append(validationErrors, fmt.Errorf("table %s: %w", table, err))
			continue
		}

		tableSchemas[table] = schema
	}

	if len(validationErrors) > 0 {
		return fmt.Errorf("schema validation failed: %v", validationErrors)
	}

	return sm.bqManager.CreateTableFromSchemas(ctx, sm.config.Broker.Topic, tableSchemas)
}

// CreateDataset implements SchemaManagerInterface
func (sm *bigQuerySchemaManager) CreateDataset(ctx context.Context, datasetID string) error {
	return sm.bqManager.CreateDataset(ctx, datasetID)
}

// Close implements SchemaManagerInterface
func (sm *bigQuerySchemaManager) Close() error {
	return sm.bqManager.Close()
}
