package pipeline

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
	"repo.nusatek.id/sugeng/walstreamer/pkg/gcloud/bigquery"
)

const (
	rateLimitDuration = 100 * time.Millisecond
)

// Pipeline represents a data pipeline for processing tables
type Pipeline struct {
	schemaManager SchemaManagerInterface
	topicManager  TopicManagerInterface
	subManager    SubscriptionManagerInterface
	logger        *logging.Logger
	config        *config.Config
}

// NewPipeline creates a new Pipeline instance
func NewPipeline(cfg *config.Config, logger *logging.Logger) (*Pipeline, error) {
	ctx := context.Background()

	creds := cfg.Broker.PubSub.CredentialsFile
	if creds == "" {
		creds = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	}

	if creds == "" {
		return nil, fmt.Errorf("GOOGLE_APPLICATION_CREDENTIALS environment variable is required")
	}

	// Initialize managers
	schemaManager, err := newBigQuerySchemaManager(ctx, cfg, logger, creds)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema manager: %w", err)
	}

	topicManager, err := newPubSubTopicManager(ctx, cfg, logger, creds)
	if err != nil {
		return nil, fmt.Errorf("failed to create topic manager: %w", err)
	}

	subManager, err := newPubSubSubscriptionManager(ctx, cfg, logger, creds)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription manager: %w", err)
	}

	return &Pipeline{
		schemaManager: schemaManager,
		topicManager:  topicManager,
		subManager:    subManager,
		logger:        logger,
		config:        cfg,
	}, nil
}

// RunPipeline executes the full pipeline setup
func (p *Pipeline) RunPipeline(ctx context.Context, configName string) error {
	p.logger.Info().Msg("Starting pipeline setup...")

	// Step 1: Create BigQuery dataset and schemas
	p.logger.Info().Msg("Creating BigQuery dataset and schemas...")
	dataset := p.config.Broker.Topic
	p.logger.Info().Str("dataset", dataset).Msg("Creating BigQuery dataset...")

	if err := p.schemaManager.CreateDataset(ctx, dataset); err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	tables := p.createSchemaTables()

	if err := p.schemaManager.CreateSchema(ctx, configName, tables); err != nil {
		return fmt.Errorf("failed to create schemas: %w", err)
	}

	// Step 2: Create Pub/Sub topics
	p.logger.Info().Msg("Creating Pub/Sub topics...")
	for _, table := range tables {
		topicID := FormatTopicID(p.config, table.Name)
		if err := p.topicManager.CreateTopic(ctx, topicID, true); err != nil {
			return fmt.Errorf("failed to create topic %s: %w", topicID, err)
		}
	}

	// Step 3: Create Pub/Sub subscriptions concurrently
	p.logger.Info().Msg("Creating Pub/Sub subscriptions concurrently...")
	if err := p.createSubscriptionsConcurrently(ctx, configName, tables); err != nil {
		return fmt.Errorf("failed to create subscriptions concurrently: %w", err)
	}

	p.logger.Info().Msg("Pipeline setup completed successfully")
	return nil
}

// createSubscriptionsConcurrently handles concurrent subscription creation
func (p *Pipeline) createSubscriptionsConcurrently(ctx context.Context, configName string, tables []bigquery.SchemaTable) error {
	var wg sync.WaitGroup
	errors := make(chan error, len(tables))
	rateLimiter := time.Tick(rateLimitDuration)

	for _, table := range tables {
		<-rateLimiter
		wg.Add(1)
		go func(table string) {
			defer wg.Done()
			subscriptionID := fmt.Sprintf("%s-%s", configName, table)
			if err := p.subManager.CreateSubscription(ctx, subscriptionID, []string{table}); err != nil {
				errors <- fmt.Errorf("failed to create subscription %s: %w", subscriptionID, err)
			}
		}(table.Name)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

// Close closes all connections
func (p *Pipeline) Close() error {
	var errors []error

	if err := p.schemaManager.Close(); err != nil {
		errors = append(errors, fmt.Errorf("failed to close schema manager: %w", err))
	}

	if err := p.topicManager.Close(); err != nil {
		errors = append(errors, fmt.Errorf("failed to close topic manager: %w", err))
	}

	if err := p.subManager.Close(); err != nil {
		errors = append(errors, fmt.Errorf("failed to close subscription manager: %w", err))
	}

	if len(errors) > 0 {
		return fmt.Errorf("errors closing pipeline: %v", errors)
	}

	return nil
}

func (p *Pipeline) createSchemaTables() []bigquery.SchemaTable {
	tables := make([]bigquery.SchemaTable, 0)

	for _, tc := range p.config.Replication.Tables {
		if !strings.HasPrefix(tc.Name, "!") && tc.Name != "" {
			tables = append(tables, bigquery.SchemaTable{
				Name:    tc.Name,
				Columns: tc.Columns,
			})
		}
	}

	return tables
}
