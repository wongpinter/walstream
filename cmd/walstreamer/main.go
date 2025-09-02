package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/wongpinter/walstreamer/config"
	"github.com/wongpinter/walstreamer/logging"
	"github.com/wongpinter/walstreamer/pkg/streamer"
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

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
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

	// Create streamer
	s, err := streamer.New(cfg, logger.Logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create streamer")
	}
	defer s.Close()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		logger.Info().Str("signal", sig.String()).Msg("Received shutdown signal")
		cancel()
	}()

	// Start streaming
	if err := s.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("Streaming failed")
	}
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
