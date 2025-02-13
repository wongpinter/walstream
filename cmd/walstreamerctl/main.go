package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"repo.nusatek.id/sugeng/walstreamer/config"
	"repo.nusatek.id/sugeng/walstreamer/logging"
	"repo.nusatek.id/sugeng/walstreamer/pkg/cleanup"
	"repo.nusatek.id/sugeng/walstreamer/pkg/gcloud/pipeline"
	"repo.nusatek.id/sugeng/walstreamer/pkg/streamer"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "walstreamerctl",
		Short: "A unified CLI for Walstreamer",
		Long:  `Walstreamerctl is a command-line tool that combines the functionality of the individual Walstreamer commands.`,
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			// Initialize Viper here, after flags are parsed
			initConfig(cmd)
		},
	}

	// Add subcommands
	rootCmd.AddCommand(cleanupCmd)
	rootCmd.AddCommand(streamCmd)
	rootCmd.AddCommand(gcloudCmd)

	// Persistent flag for config file
	rootCmd.PersistentFlags().StringP("config", "c", "config.yaml", "Path to the configuration file")
	if err := viper.BindPFlag("config", rootCmd.PersistentFlags().Lookup("config")); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func initConfig(cmd *cobra.Command) {
	configPath, _ := cmd.Flags().GetString("config")
	viper.SetConfigFile(configPath)

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if using default
			fmt.Println("Config file not found, using default")
		} else {
			// Config file was found but another error was produced
			log.Fatalf("Error reading config file: %s", err)
		}
	}
}

// Define subcommands (placeholders for now)
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Clean up Walstreamer resources in Google Cloud",
	Run: func(cmd *cobra.Command, args []string) {
		// Get config
		cfg, err := config.Load([]string{})
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}

		// Create logger config
		logCfg := logging.Config{
			Level:  logging.ParseLevel(cfg.Log.Level),
			Format: cfg.Log.Format,
		}

		// Get logger
		logger, err := logging.New(logCfg)
		if err != nil {
			log.Fatalf("Failed to create logger: %v", err)
		}

		// Parse command-line flags
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		force, _ := cmd.Flags().GetBool("force")
		interactive, _ := cmd.Flags().GetBool("interactive")

		// Create cleanup options
		opts := cleanup.Options{
			DryRun:      dryRun,
			Force:       force,
			Interactive: interactive,
		}

		// Run cleanup
		if err := cleanup.Cleanup(cfg, logger, opts); err != nil {
			logger.Error().Err(err).Msg("Cleanup failed")
			os.Exit(1)
		}

		logger.Info().Msg("Cleanup completed successfully")
	},
}

var streamCmd = &cobra.Command{
	Use:   "stream",
	Short: "Start the WAL streamer",
	Run: func(cmd *cobra.Command, args []string) {
		// Get config
		cfg, err := config.Load([]string{})
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
		}

		if err := cfg.Validate(); err != nil {
			fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
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

		// Create streamer
		s, err := streamer.New(cfg, logger.Logger)
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to create streamer")
		}
		defer func() {
			if err := s.Close(); err != nil {
				logger.Error().Err(err).Msg("Failed to close streamer")
			}
		}()

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

		// Start streaming in a goroutine with recoverability
		streamErrChan := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error().Interface("recover", r).Msg("Recovered from panic in streaming")
					streamErrChan <- fmt.Errorf("panic: %v", r)
				}
			}()
			if err := s.Start(ctx); err != nil {
				streamErrChan <- err
			} else {
				streamErrChan <- nil
			}
		}()

		// Wait for streaming to complete or an error to occur
		select {
		case err := <-streamErrChan:
			if err != nil {
				logger.Fatal().Err(err).Msg("Streaming failed")
			}
			logger.Info().Msg("Streaming completed successfully")
		case <-ctx.Done():
			logger.Info().Msg("Streaming cancelled")
		}
	},
}

var gcloudCmd = &cobra.Command{
	Use:   "onboarding",
	Short: "Set up Google Cloud resources",
	Run: func(cmd *cobra.Command, args []string) {
		// Get config
		cfg, err := config.Load([]string{})
		if err != nil {
			log.Fatalf("Failed to load config: %v", err)
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
	},
}
