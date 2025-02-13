package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
	"repo.nusatek.id/sugeng/walstreamer/pkg/gcloud/pipeline"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	files := strings.Split(*configFile, ",")

	// Load configuration
	env := "dev" // or "prod"
	configFiles := getConfigFiles(env, files)

	// Load configuration
	cfg, err := config.Load(configFiles)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	loggerConfig := logging.Config{
		Level:  logging.ParseLevel(cfg.Log.Level),
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

	if err := pipelineManager.RunPipeline(ctx, "default"); err != nil {
		if err == context.Canceled {
			logger.Info().Msg("Pipeline shutdown gracefully")
			return
		}
		logger.Fatal().Err(err).Msg("Pipeline execution failed")
	}

	logger.Info().Msg("Successfully created/updated GCloud resources")
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
