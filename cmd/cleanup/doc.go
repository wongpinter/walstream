// Package main provides a command line tool for cleaning up PostgreSQL replication resources.
//
// This tool safely removes existing publications and replication slots that were created
// by the walstreamer service. It handles active connections and ensures proper cleanup.
//
// Usage:
//
//	cleanup -config config.yaml
//
// The config file specifies:
// - Database connection details
// - Publication name to remove
// - Replication slot name to remove
// - Logging settings
package main
