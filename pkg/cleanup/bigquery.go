package cleanup

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/wongpinter/walstreamer/config"
	"github.com/wongpinter/walstreamer/logging"
)

// bigQueryCleaner implements BigQueryCleanerInterface
type bigQueryCleaner struct {
	client    *bigquery.Client
	logger    *logging.Logger
	config    *config.Config
	projectID string
}

// NewBigQueryCleaner creates a new BigQuery cleaner
func NewBigQueryCleaner(ctx context.Context, cfg *config.Config, credentials string, logger *logging.Logger) (BigQueryCleanerInterface, error) {
	client, err := bigquery.NewClient(ctx, cfg.Broker.PubSub.ProjectID, option.WithCredentialsFile(credentials))
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %w", err)
	}

	return &bigQueryCleaner{
		client:    client,
		logger:    logger.WithComponent("bigquery-cleaner"),
		config:    cfg,
		projectID: cfg.Broker.PubSub.ProjectID,
	}, nil
}

// ListResources implements BigQueryCleanerInterface
func (bc *bigQueryCleaner) ListResources(ctx context.Context) ([]ResourceInfo, error) {
	var resources []ResourceInfo

	// Get configured dataset
	dataset := bc.client.Dataset(bc.config.Broker.Topic)
	_, err := dataset.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
			bc.logger.Debug().Str("dataset", bc.config.Broker.Topic).Msg("Dataset does not exist")
			return nil, nil
		}
		return nil, fmt.Errorf("error checking dataset existence: %w", err)
	}

	// Add dataset resource
	datasetInfo := ResourceInfo{
		Type:    "dataset",
		Name:    bc.config.Broker.Topic,
		Project: bc.projectID,
	}

	// Get configured tables
	var tableNames []string
	for _, tablePattern := range bc.config.Replication.GetTableNames() {
		// Extract table name from pattern (e.g., "public.users" -> "users")
		parts := strings.Split(tablePattern, ".")
		if len(parts) >= 2 {
			tableNames = append(tableNames, parts[1])
		} else {
			tableNames = append(tableNames, parts[0])
		}
	}

	for _, tableName := range tableNames {
		table := dataset.Table(tableName)
		_, err := table.Metadata(ctx)
		if err != nil {
			if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
				bc.logger.Debug().Str("table", tableName).Msg("Table does not exist")
				continue
			}
			return nil, fmt.Errorf("error checking table %s: %w", tableName, err)
		}

		tableInfo := ResourceInfo{
			Type:      "table",
			Name:      fmt.Sprintf("%s.%s", bc.config.Broker.Topic, tableName),
			Project:   bc.projectID,
			DependsOn: []string{bc.config.Broker.Topic},
		}
		datasetInfo.RequiredFor = append(datasetInfo.RequiredFor, tableInfo.Name)
		resources = append(resources, tableInfo)
	}

	resources = append(resources, datasetInfo)
	return resources, nil
}

// DeleteDataset implements BigQueryCleanerInterface
func (bc *bigQueryCleaner) DeleteDataset(ctx context.Context, name string) error {
	dataset := bc.client.Dataset(name)

	// Verify dataset exists
	metadata, err := dataset.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
			bc.logger.Debug().Str("dataset", name).Msg("Dataset does not exist")
			return nil
		}
		return fmt.Errorf("error checking dataset %s: %w", name, err)
	}

	bc.logger.Info().
		Str("dataset", name).
		Str("location", metadata.Location).
		Time("created", metadata.CreationTime).
		Msg("Deleting dataset")

	// Delete dataset with force option to delete all tables
	if err := dataset.DeleteWithContents(ctx); err != nil {
		return fmt.Errorf("error deleting dataset %s: %w", name, err)
	}

	return nil
}

// DeleteTable implements BigQueryCleanerInterface
func (bc *bigQueryCleaner) DeleteTable(ctx context.Context, dataset, table string) error {
	tableRef := bc.client.Dataset(dataset).Table(table)

	// Verify table exists
	metadata, err := tableRef.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
			bc.logger.Debug().Str("dataset", dataset).Str("table", table).Msg("Table does not exist")
			return nil
		}
		return fmt.Errorf("error checking table %s.%s: %w", dataset, table, err)
	}

	bc.logger.Info().
		Str("dataset", dataset).
		Str("table", table).
		Time("created", metadata.CreationTime).
		Uint64("rows", metadata.NumRows).
		Msg("Deleting table")

	if err := tableRef.Delete(ctx); err != nil {
		return fmt.Errorf("error deleting table %s.%s: %w", dataset, table, err)
	}

	return nil
}

// VerifyResourceExists implements BigQueryCleanerInterface
func (bc *bigQueryCleaner) VerifyResourceExists(ctx context.Context, resType, name string) (bool, error) {
	switch resType {
	case "dataset":
		_, err := bc.client.Dataset(name).Metadata(ctx)
		if err != nil {
			if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
				return false, nil
			}
			return false, fmt.Errorf("error checking dataset %s: %w", name, err)
		}
		return true, nil

	case "table":
		// For tables, name should be in format "dataset.table"
		dataset, table := SplitTableName(name)
		if dataset == "" || table == "" {
			return false, fmt.Errorf("invalid table name format: %s, expected 'dataset.table'", name)
		}
		_, err := bc.client.Dataset(dataset).Table(table).Metadata(ctx)
		if err != nil {
			if e, ok := err.(*googleapi.Error); ok && e.Code == 404 {
				return false, nil
			}
			return false, fmt.Errorf("error checking table %s: %w", name, err)
		}
		return true, nil

	default:
		return false, fmt.Errorf("unsupported resource type: %s", resType)
	}
}

// Close implements BigQueryCleanerInterface
func (bc *bigQueryCleaner) Close() error {
	if err := bc.client.Close(); err != nil {
		return fmt.Errorf("error closing BigQuery client: %w", err)
	}
	return nil
}
