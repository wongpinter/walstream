package cleanup

import (
	"context"
	"strings"
)

// Options holds cleanup pipeline configuration
type Options struct {
	// DryRun if true, only shows what would be deleted without actually deleting
	DryRun bool
	// Force if true, skips confirmation prompts
	Force bool
	// Interactive if true, prompts for confirmation before each deletion
	Interactive bool
}

// ResourceInfo holds information about a resource to be cleaned up
type ResourceInfo struct {
	Type        string // "dataset", "table", "topic", "subscription"
	Name        string
	Project     string
	DependsOn   []string // Names of resources this depends on
	RequiredFor []string // Names of resources that depend on this
}

// CleanupResult holds the result of a cleanup operation
type CleanupResult struct {
	Resource ResourceInfo
	Success  bool
	Error    error
	Skipped  bool
	Message  string
}

// BigQueryCleanerInterface handles cleanup of BigQuery resources
type BigQueryCleanerInterface interface {
	// ListResources returns all BigQuery resources that would be affected
	ListResources(ctx context.Context) ([]ResourceInfo, error)
	// DeleteDataset deletes a dataset and all its tables
	DeleteDataset(ctx context.Context, name string) error
	// DeleteTable deletes a specific table
	DeleteTable(ctx context.Context, dataset, table string) error
	// VerifyResourceExists checks if a BigQuery resource exists
	VerifyResourceExists(ctx context.Context, resType, name string) (bool, error)
	// Close cleans up any resources
	Close() error
}

// PubSubCleanerInterface handles cleanup of Pub/Sub resources
type PubSubCleanerInterface interface {
	// ListResources returns all Pub/Sub resources that would be affected
	ListResources(ctx context.Context) ([]ResourceInfo, error)
	// DeleteTopic deletes a topic and its subscriptions
	DeleteTopic(ctx context.Context, name string) error
	// DeleteSubscription deletes a subscription
	DeleteSubscription(ctx context.Context, name string) error
	// VerifyResourceExists checks if a Pub/Sub resource exists
	VerifyResourceExists(ctx context.Context, resType, name string) (bool, error)
	// Close cleans up any resources
	Close() error
}

// DependencyResolver handles resource deletion order
type DependencyResolver interface {
	// AddResource adds a resource to the dependency graph
	AddResource(res ResourceInfo)
	// GetDeletionOrder returns resources in safe deletion order
	GetDeletionOrder() []ResourceInfo
	// ValidateNoCycles ensures there are no circular dependencies
	ValidateNoCycles() error
}

// ResultCollector accumulates cleanup results
type ResultCollector interface {
	// AddResult adds a cleanup result
	AddResult(result CleanupResult)
	// GetResults returns all cleanup results
	GetResults() []CleanupResult
	// HasErrors returns true if any cleanup operations failed
	HasErrors() bool
	// GetSummary returns a human-readable summary of the cleanup operation
	GetSummary() string
}

// Utility functions

// SplitTableName splits a table name in format "dataset.table"
func SplitTableName(name string) (dataset, table string) {
	parts := strings.Split(name, ".")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", ""
}
