package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
	"repo.nusatek.id/sugeng/walstreamer/pkg/cleanup"
	"repo.nusatek.id/sugeng/walstreamer/replication"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	dryrun := flag.Bool("dryrun", false, "Run cleanup in dry-run mode")
	force := flag.Bool("force", false, "Force cleanup")

	flag.Parse()

	// Load configuration
	cfg, err := config.Load(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
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
		PublicationName: cfg.Replication.PublicationName,
		SlotName:        cfg.Replication.SlotName,
	})

	// Connect to database
	if err := replManager.Connect(context.Background(), cfg.Database.GetDSN()); err != nil {
		logger.Fatal().Err(err).Msg("Failed to connect to database")
	}
	defer replManager.Close(context.Background())

	// Clean up resources
	logger.Info().
		Str("publication", cfg.Replication.PublicationName).
		Str("slot", cfg.Replication.SlotName).
		Msg("Cleaning up replication resources")

	if *dryrun {
		if err := replManager.Cleanup(context.Background()); err != nil {
			logger.Fatal().Err(err).Msg("Failed to cleanup resources")
		}
	}

	logger.Info().Msg("Successfully cleaned up replication resources")
}
