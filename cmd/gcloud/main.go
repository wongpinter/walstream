package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
	"repo.nusatek.id/sugeng/walstreamer/pkg/gcloud/pipeline"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
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

	// Set up context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create pipeline manager
	pipelineManager, err := pipeline.NewPipeline(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create pipeline manager")
	}
	defer pipelineManager.Close()

	if err := pipelineManager.RunPipeline(ctx, "default", cfg.GetIncludedTables()); err != nil {
		if err == context.Canceled {
			logger.Info().Msg("Pipeline shutdown gracefully")
			return
		}
		logger.Fatal().Err(err).Msg("Pipeline execution failed")
	}

	logger.Info().Msg("Successfully created/updated GCloud resources")
}
