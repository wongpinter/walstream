package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"

	"repo.nusatek.id/sugeng/walstreamer/broker/inmemory"
	"repo.nusatek.id/sugeng/walstreamer/model"
	"repo.nusatek.id/sugeng/walstreamer/replication"
	"repo.nusatek.id/sugeng/walstreamer/wal"
)

func main() {
	// Initialize logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Define connection strings
	baseConnString := "postgres://guest:guest@Cosmic-Echo.local:5432/local?sslmode=disable"
	replConnString := baseConnString + "&replication=database"

	// Create replication configuration
	replConfig := replication.Config{
		PublicationName: "walstreamer_pub",
		SlotName:        "walstreamer_slot",
		IncludedTables:  []string{}, // empty means all tables
		ExcludedTables:  []string{}, // no excluded tables
	}

	// Initialize replication manager
	replManager := replication.NewManager(replConfig)
	if err := replManager.Connect(context.Background(), baseConnString); err != nil {
		logger.Fatal().Err(err).Msg("failed to connect replication manager")
	}
	defer replManager.Close(context.Background())

	// Setup replication (create publication and slot)
	if err := replManager.Setup(context.Background()); err != nil {
		logger.Fatal().Err(err).Msg("failed to setup replication")
	}

	// Initialize broker
	broker := inmemory.NewInMemoryBroker()

	// Create WAL reader configuration
	walConfig := wal.Config{
		ConnString:      replConnString,
		PublicationName: replConfig.PublicationName,
		SlotName:        replConfig.SlotName,
		StandbyTimeout:  10 * time.Second,
		Logger:          logger,
	}

	// Create message handler that publishes to broker
	messageHandler := func(msg *model.Message) error {
		// Print message details for debugging
		logger.Info().
			Str("operation", msg.Operation).
			Str("schema", msg.Schema).
			Str("table", msg.Table).
			Uint64("lsn", msg.LSN).
			Int64("timestamp", msg.Timestamp).
			Interface("before", msg.Before).
			Interface("after", msg.After).
			Msg("received WAL message")

		// Publish message to broker
		return broker.Publish(context.Background(), msg)
	}

	// Create WAL reader
	reader := wal.NewReader(walConfig, messageHandler)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\nReceived signal: %v\n", sig)
		cancel()
	}()

	// Start reading WAL
	fmt.Println("Starting WAL reader...")
	if err := reader.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("WAL reader failed")
	}
}
