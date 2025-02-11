// Package main provides a command line tool for setting up Google Cloud resources.
//
// This tool creates or updates the necessary Google Cloud resources for the walstreamer
// service, including:
// - PubSub topics for each monitored table
// - BigQuery schemas for data storage
//
// Usage:
//
//	gcloud -config config.yaml
//
// The config file specifies:
// - Google Cloud project settings
// - Tables and their corresponding topics
// - Credentials and authentication
// - Logging settings
package main
