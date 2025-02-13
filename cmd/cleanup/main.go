package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
	"repo.nusatek.id/sugeng/walstreamer/pkg/cleanup"
	"repo.nusatek.id/sugeng/walstreamer/replication"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "config.yaml", "Comma separated list of config files")
	dryrun := flag.Bool("dryrun", false, "Run cleanup in dry-run mode")
	force := flag.Bool("force", false, "Force cleanup")
	flag.Parse()

	files := strings.Split(*configFile, ",")

	// Load configuration
	env := "dev" // or "prod"
	configFiles := getConfigFiles(env, files)

	cfg, err := config.Load(configFiles)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
	}

	// Initialize logger
	loggerConfig := logging.Config{
		Level:  logging.Level(cfg.Log.Level),
		Format: cfg.Log.Format,
	}
	logger, err := logging.New(loggerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	cleanUpPipeline, err := cleanup.NewPipeline(cfg, logger, cleanup.Options{
		Force:  *force,
		DryRun: *dryrun,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create cleanup pipeline")
	}

	if err := cleanUpPipeline.Run(context.Background()); err != nil {
		logger.Fatal().Err(err).Msg("Failed to run cleanup pipeline")
	}

	// Create replication manager
	replManager := replication.NewManager(replication.Config{
		PublicationName: cfg.Replication.Publication,
		SlotName:        cfg.Replication.Slot,
	})

	// Connect to database
	if err := replManager.Connect(context.Background(), cfg.Database.GetDSN()); err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer replManager.Close(context.Background())

	// Clean up resources
	logger.Info().
		Str("publication", cfg.Replication.Publication).
		Str("slot", cfg.Replication.Slot).
		Msg("Cleaning up replication resources")

	if *dryrun {
		if err := replManager.Cleanup(context.Background()); err != nil {
			logger.Fatal().Err(err).Msg("Failed to cleanup resources")
		}
	}

	logger.Info().Msg("Successfully cleaned up replication resources")
}

func getConfigFiles(env string, files []string) []string {
	baseFiles := []string{
		// "config/database.yaml",
		// "config/replication.yaml",
		// "config/broker.yaml",
		// "config/log.yaml",
		// "config/lsn.yaml",
		// "config/storage.yaml",
	}

	// Add user-specified files
	baseFiles = append(baseFiles, files...)

	// Add environment-specific overrides
	switch env {
	case "dev":
		baseFiles = append(baseFiles, "config/dev-overrides.yaml")
	case "prod":
		baseFiles = append(baseFiles, "config/prod-overrides.yaml")
	}

	return baseFiles
}
