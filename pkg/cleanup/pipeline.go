package cleanup

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
)

// Pipeline represents a cleanup pipeline for removing infrastructure resources
type Pipeline struct {
	bqCleaner BigQueryCleanerInterface
	psCleaner PubSubCleanerInterface
	logger    *logging.Logger
	config    *config.Config
	collector ResultCollector
	resolver  DependencyResolver
	opts      Options
}

// NewPipeline creates a new cleanup pipeline
func NewPipeline(cfg *config.Config, logger *logging.Logger, opts Options) (*Pipeline, error) {
	ctx := context.Background()

	// Get Google Cloud credentials file from config
	creds := cfg.Broker.PubSub.CredentialsFile
	if creds == "" {
		creds = os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	}
	if creds == "" {
		return nil, fmt.Errorf("GOOGLE_APPLICATION_CREDENTIALS environment variable is required")
	}

	// Initialize cleaners
	bqCleaner, err := NewBigQueryCleaner(ctx, cfg, creds, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery cleaner: %w", err)
	}

	psCleaner, err := NewPubSubCleaner(ctx, cfg, creds, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pub/Sub cleaner: %w", err)
	}

	return &Pipeline{
		bqCleaner: bqCleaner,
		psCleaner: psCleaner,
		logger:    logger.WithComponent("cleanup-pipeline"),
		config:    cfg,
		collector: NewResultCollector(),
		resolver:  NewDependencyResolver(),
		opts:      opts,
	}, nil
}

// Cleanup performs the cleanup of resources based on the provided configuration and options.
func Cleanup(cfg *config.Config, logger *logging.Logger, opts Options) error {
	pipeline, err := NewPipeline(cfg, logger, opts)
	if err != nil {
		return fmt.Errorf("failed to create cleanup pipeline: %w", err)
	}

	if err := pipeline.Run(context.Background()); err != nil {
		return fmt.Errorf("failed to run cleanup pipeline: %w", err)
	}

	return nil
}

// Run executes the cleanup pipeline
func (p *Pipeline) Run(ctx context.Context) error {
	// Step 1: List all resources
	if err := p.gatherResources(ctx); err != nil {
		return fmt.Errorf("failed to gather resources: %w", err)
	}

	// Step 2: Validate dependencies
	if err := p.resolver.ValidateNoCycles(); err != nil {
		return fmt.Errorf("invalid resource dependencies: %w", err)
	}

	// Step 3: Get deletion order
	resources := p.resolver.GetDeletionOrder()

	if len(resources) == 0 {
		p.logger.Info().Msg("No resources found to clean up")
		return nil
	}

	// If in dry run mode, just show what would be deleted
	if p.opts.DryRun {
		p.showDryRun(resources)
		return nil
	}

	// Step 4: Confirm deletion if not forced
	if !p.opts.Force {
		if !p.confirmDeletion(resources) {
			return fmt.Errorf("cleanup cancelled by user")
		}
	}

	// Step 5: Delete resources in order
	for _, res := range resources {
		// Skip resources with empty type or name
		if res.Type == "" || res.Name == "" {
			p.logger.Warn().
				Str("type", res.Type).
				Str("name", res.Name).
				Msg("Skipping resource with empty type or name")
			continue
		}

		if err := p.deleteResource(ctx, res); err != nil {
			p.collector.AddResult(CleanupResult{
				Resource: res,
				Success:  false,
				Error:    err,
			})
			p.logger.Error().Err(err).
				Str("type", res.Type).
				Str("name", res.Name).
				Msg("Failed to delete resource")
			if !p.opts.Force {
				return fmt.Errorf("failed to delete %s %s: %w", res.Type, res.Name, err)
			}
		}
	}

	// Print summary
	p.logger.Info().Msg("\n" + p.collector.GetSummary())
	return nil
}

// gatherResources lists all resources and builds the dependency graph
func (p *Pipeline) gatherResources(ctx context.Context) error {
	// Get BigQuery resources
	bqResources, bqErr := p.bqCleaner.ListResources(ctx)
	if bqErr != nil {
		return fmt.Errorf("failed to list BigQuery resources: %w", bqErr)
	}
	for _, res := range bqResources {
		if res.Type != "" && res.Name != "" {
			p.resolver.AddResource(res)
		} else {
			p.logger.Warn().
				Str("type", res.Type).
				Str("name", res.Name).
				Msg("Skipping invalid BigQuery resource")
		}
	}

	// Get Pub/Sub resources
	psResources, psErr := p.psCleaner.ListResources(ctx)
	if psErr != nil {
		return fmt.Errorf("failed to list Pub/Sub resources: %w", psErr)
	}
	for _, res := range psResources {
		if res.Type != "" && res.Name != "" {
			p.resolver.AddResource(res)
		} else {
			p.logger.Warn().
				Str("type", res.Type).
				Str("name", res.Name).
				Msg("Skipping invalid Pub/Sub resource")
		}
	}

	return nil
}

// showDryRun displays what would be deleted without actually deleting
func (p *Pipeline) showDryRun(resources []ResourceInfo) {
	fmt.Println("\nDRY RUN - The following resources would be deleted:")
	for _, res := range resources {
		fmt.Printf("- [%s] %s\n", res.Type, res.Name)
		if len(res.DependsOn) > 0 {
			fmt.Printf("  Depends on: %s\n", strings.Join(res.DependsOn, ", "))
		}
		if len(res.RequiredFor) > 0 {
			fmt.Printf("  Required for: %s\n", strings.Join(res.RequiredFor, ", "))
		}
	}
	fmt.Println("\nNo resources were actually deleted.")
}

// confirmDeletion asks for user confirmation before deleting resources
func (p *Pipeline) confirmDeletion(resources []ResourceInfo) bool {
	fmt.Println("\nThe following resources will be deleted:")
	for _, res := range resources {
		fmt.Printf("- [%s] %s\n", res.Type, res.Name)
	}
	fmt.Print("\nAre you sure you want to proceed? (yes/no): ")

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		p.logger.Error().Err(err).Msg("Failed to read user input")
		return false
	}

	return strings.ToLower(strings.TrimSpace(input)) == "yes"
}

// deleteResource deletes a single resource based on its type
func (p *Pipeline) deleteResource(ctx context.Context, res ResourceInfo) error {
	if res.Type == "" || res.Name == "" {
		return fmt.Errorf("invalid resource: type or name is empty")
	}

	// Verify resource still exists
	exists, verifyErr := p.verifyResourceExists(ctx, res)
	if verifyErr != nil {
		return fmt.Errorf("failed to verify resource existence: %w", verifyErr)
	}
	if !exists {
		p.collector.AddResult(CleanupResult{
			Resource: res,
			Success:  true,
			Skipped:  true,
			Message:  "Resource does not exist",
		})
		return nil
	}

	// Interactive mode: confirm each deletion
	if p.opts.Interactive {
		if !p.confirmResourceDeletion(res) {
			p.collector.AddResult(CleanupResult{
				Resource: res,
				Success:  true,
				Skipped:  true,
				Message:  "Skipped by user",
			})
			return nil
		}
	}

	var deleteErr error
	switch res.Type {
	case "dataset":
		deleteErr = p.bqCleaner.DeleteDataset(ctx, res.Name)
	case "table":
		dataset, table := SplitTableName(res.Name)
		if dataset == "" || table == "" {
			return fmt.Errorf("invalid table name format: %s, expected 'dataset.table'", res.Name)
		}
		deleteErr = p.bqCleaner.DeleteTable(ctx, dataset, table)
	case "topic":
		deleteErr = p.psCleaner.DeleteTopic(ctx, res.Name)
	case "subscription":
		deleteErr = p.psCleaner.DeleteSubscription(ctx, res.Name)
	default:
		return fmt.Errorf("unsupported resource type: %s", res.Type)
	}

	if deleteErr != nil {
		return fmt.Errorf("failed to delete %s %s: %w", res.Type, res.Name, deleteErr)
	}

	p.collector.AddResult(CleanupResult{
		Resource: res,
		Success:  true,
	})
	return nil
}

// verifyResourceExists checks if a resource still exists before attempting deletion
func (p *Pipeline) verifyResourceExists(ctx context.Context, res ResourceInfo) (bool, error) {
	if res.Type == "" {
		return false, fmt.Errorf("resource type is empty")
	}
	if res.Name == "" {
		return false, fmt.Errorf("resource name is empty")
	}

	switch {
	case res.Type == "dataset" || res.Type == "table":
		return p.bqCleaner.VerifyResourceExists(ctx, res.Type, res.Name)
	case res.Type == "topic" || res.Type == "subscription":
		return p.psCleaner.VerifyResourceExists(ctx, res.Type, res.Name)
	default:
		return false, fmt.Errorf("unsupported resource type: %s", res.Type)
	}
}

// confirmResourceDeletion asks for confirmation before deleting a specific resource
func (p *Pipeline) confirmResourceDeletion(res ResourceInfo) bool {
	fmt.Printf("\nDelete %s '%s'? (yes/no): ", res.Type, res.Name)

	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		p.logger.Error().Err(err).Msg("Failed to read user input")
		return false
	}

	return strings.ToLower(strings.TrimSpace(input)) == "yes"
}

// Close closes all cleaners
func (p *Pipeline) Close() error {
	var errs []error

	if err := p.bqCleaner.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close BigQuery cleaner: %w", err))
	}

	if err := p.psCleaner.Close(); err != nil {
		errs = append(errs, fmt.Errorf("failed to close Pub/Sub cleaner: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing cleanup pipeline: %v", errs)
	}

	return nil
}
