// Package main provides the main entry point for the walstreamer CDC service.
//
// The walstreamer service captures changes from PostgreSQL using logical replication
// and streams them to a configured message broker (NATS, Google PubSub, or in-memory).
//
// Usage:
//
//	walstreamer -config config.yaml
//
// The config file specifies:
// - Database connection details
// - Replication settings (publication name, slot name)
// - Tables to monitor
// - Message broker configuration
// - Logging settings
package main
