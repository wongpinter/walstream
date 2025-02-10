# Walstreamer

Walstreamer is a high-performance Change Data Capture (CDC) tool written in Go that captures changes from PostgreSQL's Write-Ahead Log (WAL) and streams them to various message brokers. It's designed to be reliable, configurable, and easy to integrate with existing data pipelines.

## Features

- **PostgreSQL WAL Streaming**

  - Real-time change capture using logical replication
  - Support for INSERT, UPDATE, and DELETE operations
  - Table-level filtering and operation filtering
  - Initial table sync capability
  - Automatic reconnection with exponential backoff

- **Multiple Message Broker Support**

  - NATS
  - Google Cloud Pub/Sub
  - In-memory broker (for testing)

- **Per-Table Configuration**

  - Custom operation filtering per table
  - Table-specific message topics
  - Flexible include/exclude patterns

- **Robust Error Handling**

  - Automatic reconnection to PostgreSQL
  - Configurable retry mechanisms
  - Detailed logging with different log levels

- **LSN (Log Sequence Number) Tracking**
  - Persistent LSN storage
  - Multiple storage backends (file, PostgreSQL)
  - Configurable persistence interval

## Prerequisites

- Go 1.19 or later
- PostgreSQL 10 or later with logical replication enabled
- One of the supported message brokers:
  - NATS Server
  - Google Cloud Pub/Sub account and credentials
  - (or use in-memory broker for testing)

## Installation

```bash
go get repo.nusatek.id/sugeng/walstreamer
```

## Configuration

Walstreamer uses a YAML configuration file. Here's a complete example with all available options:

```yaml
# Database configuration
database:
  host: localhost
  port: 5432
  user: postgres
  password: secret
  dbname: mydb
  sslmode: disable

# Log configuration
log:
  level: info
  format: console

# Replication configuration
replication:
  publication_name: walstreamer_pub
  slot_name: walstreamer_slot
  standby_timeout: 10

  # List of tables to replicate with their configurations
  tables:
    - name: public.inventory
      operations: ["INSERT", "UPDATE", "DELETE"]
      topic: inventory
    - name: public.users
      operations: ["INSERT", "UPDATE", "DELETE"]
      topic: users

  # Default operations if not specified in table config
  default_ops:
    - INSERT
    - UPDATE
    - DELETE

  reconnect:
    max_attempts: 0
    initial_delay: 1
    max_delay: 30

  initial_sync: true
  batch_size: 1000

# Message broker configuration
broker:
  type: nats # Options: inmemory, nats, pubsub

  # NATS configuration
  hosts:
    - nats://localhost:4222
  topic: walstreamer.changes
  username: ""
  password: ""

  # Google Cloud Pub/Sub configuration
  pubsub:
    project_id: "your-project-id"
    topic_prefix: "walstreamer-"
    credentials_file: "/path/to/credentials.json"
    auto_create_topic: true

# LSN persistence configuration
lsn:
  type: file
  path: /tmp/walstreamer/lsn
  persist_interval: 5s

# Storage configuration
storage:
  type: file
  path: /tmp/walstreamer
```

## Usage

### Basic Usage

1. Create a configuration file:

```bash
cp config.example.yaml config.yaml
```

2. Edit the configuration file to match your environment:

```bash
vim config.yaml
```

3. Run walstreamer:

```bash
walstreamer -config config.yaml
```

### PostgreSQL Setup

1. Enable logical replication in `postgresql.conf`:

```
wal_level = logical
```

2. Create a publication for the tables you want to stream:

```sql
CREATE PUBLICATION walstreamer_pub FOR TABLE inventory, users;
```

3. Grant necessary permissions to the database user:

```sql
GRANT SELECT ON ALL TABLES IN SCHEMA public TO walstreamer_user;
GRANT REPLICATION CLIENT ON DATABASE mydb TO walstreamer_user;
```

### Message Format

Messages are published in JSON format with the following structure:

```json
{
  "operation": "INSERT",
  "table": "public.users",
  "schema": {
    "columns": [
      {
        "name": "id",
        "type": "integer",
        "primary": true
      },
      {
        "name": "name",
        "type": "text"
      }
    ]
  },
  "before": null,
  "after": {
    "id": 1,
    "name": "John Doe"
  },
  "timestamp": "2025-02-10T09:53:15Z",
  "lsn": "0/1620B20"
}
```

## Examples

### Streaming to NATS

```yaml
broker:
  type: nats
  hosts:
    - nats://localhost:4222
  topic: walstreamer.changes
```

Subscribe to changes using the NATS client:

```go
nc, _ := nats.Connect("nats://localhost:4222")
sub, _ := nc.Subscribe("walstreamer.changes", func(msg *nats.Msg) {
    fmt.Printf("Received: %s\n", string(msg.Data))
})
```

### Streaming to Google Cloud Pub/Sub

```yaml
broker:
  type: pubsub
  pubsub:
    project_id: "my-project"
    topic_prefix: "walstreamer-"
    credentials_file: "/path/to/credentials.json"
    auto_create_topic: true
```

Each table will have its own topic with the configured prefix:

- `walstreamer-inventory`
- `walstreamer-users`

### Initial Table Sync

Enable initial sync to capture existing data before streaming changes:

```yaml
replication:
  initial_sync: true
  batch_size: 1000
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
