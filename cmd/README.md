# Walstreamer Command Line Tools

This directory contains the command line tools for the Walstreamer package.

## Commands

### walstreamer

The main command for running the PostgreSQL Change Data Capture (CDC) streamer.

```bash
go run cmd/walstreamer/main.go -config config.yaml
```

### cleanup

Command to clean up existing replication slots and publications.

```bash
go run cmd/cleanup/main.go -config config.yaml
```

### gcloud

Command to set up Google Cloud resources (PubSub topics, subscriptions, and BigQuery schemas).

```bash
go run cmd/gcloud/main.go -config config.yaml
```

## Configuration

All commands use the same configuration file format. See `config.yaml` in the root directory for an example configuration.
