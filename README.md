# Walstreamer

Walstreamer is a robust and efficient tool designed to stream Write-Ahead Logging (WAL) data from PostgreSQL databases to various destinations, such as Google Cloud Pub/Sub, BigQuery, NATS, RabbitMQ, or in-memory brokers. It leverages PostgreSQL's logical replication capabilities to capture changes in real-time and provides a flexible architecture for processing and delivering these changes.

## Features

*   **Real-time WAL Streaming:** Captures changes from PostgreSQL's WAL using logical replication.
*   **Multiple Brokers:** Supports various message brokers for delivering WAL data:
    *   Google Cloud Pub/Sub
    *   NATS
    *   RabbitMQ
    *   In-memory (for testing and development)
*   **Google Cloud Integration:**
    *   Seamlessly integrates with Google Cloud Pub/Sub and BigQuery.
    *   Automated resource provisioning (Pub/Sub topics, subscriptions, BigQuery datasets, and tables) via the `onboarding` command.
    *   Resource cleanup via the `cleanup` command.
*   **Data Transformation:** Converts PostgreSQL data types to appropriate formats for the target broker (e.g., BigQuery).
*   **Initial Data Synchronization:**  Provides a mechanism for initial data synchronization (snapshotting) to ensure consistency between the source database and the target.
*   **Configuration:** Highly configurable via a YAML configuration file (`config.yaml` by default).
*   **Logging:** Uses Zerolog for structured and efficient logging.
*   **CLI Tool (`walstreamerctl`):** A unified command-line interface for managing Walstreamer, including streaming, Google Cloud resource management, and cleanup.
*   **Resilient:** Handles connection interruptions and automatically reconnects to PostgreSQL.  Includes panic recovery for the streaming process.
*   **Extensible:** Designed with a modular architecture, making it easy to add support for new brokers and data transformations.

## Architecture

Walstreamer's core components include:

1.  **Replication:**  Connects to PostgreSQL using logical replication and reads WAL data.
2.  **Decoder:** Decodes the raw WAL data into a structured format (using `pgoutput` plugin).
3.  **Broker:**  An interface for interacting with different message brokers (Pub/Sub, NATS, RabbitMQ, In-memory).
4.  **Publisher:** Publishes the decoded WAL messages to the chosen broker.
5.  **Streamer:**  The main component that orchestrates the entire streaming process.
6.  **GCloud**  Components for managing Google Cloud resources (Pub/Sub, BigQuery).
7. **Cleanup:** Component for removing created resources.

## Configuration

Walstreamer is configured using a YAML file (default: `config.yaml`).  The configuration file allows you to specify:

*   **PostgreSQL Connection:**  Details for connecting to the PostgreSQL database (host, port, user, password, database name, replication slot, publication name).
*   **Broker Settings:** Configuration for the chosen message broker (Pub/Sub project ID, topic ID, etc.; NATS URL; RabbitMQ connection details).
*   **BigQuery Settings (Optional):**  Project ID, dataset ID, table ID, and schema mapping.
*   **Logging:** Log level and format.
*   **Initial Sync (Optional):** Settings for performing an initial data synchronization.
*   **Table Filtering (Optional):**  Specify which tables to include or exclude from streaming.

**Example `config.yaml` (Pub/Sub):**

```yaml
# Database configuration
database:
  host: postgres-server.example.com # PostgreSQL server hostname
  port: 5432 # PostgreSQL server port (default: 5432)
  user: postgres_user # PostgreSQL username
  password: your_secure_password_here # PostgreSQL password
  dbname: example_db # PostgreSQL database name
  sslmode: disable # SSL mode (disable, require, verify-ca, verify-full)

# Log configuration
log:
  level: info # Log level (debug, info, warn, error)
  format: console # Log format (console, json)

# Replication configuration
replication:
  publication: example_publication # PostgreSQL publication name
  slot: example_replication_slot # PostgreSQL replication slot name
  standby_timeout: 10 # Timeout for standby status updates (in seconds)
  initial_sync: true # Whether to perform initial table sync
  batch_size: 500 # Number of changes to process in a batch

  # Reconnection settings for PostgreSQL
  reconnect:
    max_attempts: 0 # Maximum reconnection attempts (0 means unlimited)
    initial_delay: 1 # Initial delay between reconnection attempts (in seconds)
    max_delay: 30 # Maximum delay between reconnection attempts (in seconds)

  # List of tables to replicate with their configurations
  tables:
    example_table_1:
      operations: ["INSERT", "UPDATE", "DELETE"] # Allowed operations to replicate
      topic: example_topic_1 # Destination topic name
      columns: # List of columns to replicate
        - id
        - created_at
        - updated_at
        - status

    example_table_2:
      operations: ["INSERT", "UPDATE"]
      topic: example_topic_2
      columns:
        - id
        - name
        - value
        - category

# Message broker configuration
broker:
  type: pubsub # Broker type (Options: inmemory, nats, pubsub)
  topic: default_topic # Default topic name (used for tables without specific topic)

  # Google Cloud Pub/Sub specific configuration
  pubsub:
    project_id: your-gcp-project-id # Google Cloud project ID
    topic_prefix: example_prefix # Prefix for auto-generated topics
    credentials_file: path/to/credentials.json # Path to service account JSON key
    auto_create_topic: true # Automatically create topics if they don't exist
    location: your-region # GCP resource location

# LSN (Log Sequence Number) persistence configuration
lsn:
  type: file # LSN storage type (file, postgres)
  path: /path/to/lsn/file # Path to LSN storage file
  persist_interval: 30s # How often to persist LSN to storage (e.g., 5s, 1m)

# Storage configuration
storage:
  type: file # Storage type (file)
  path: /path/to/storage/directory # Path to storage directory
```

**Configuration Options:**

*   **`log`:**
    *   `level`:  Logging level (`debug`, `info`, `warn`, `error`, `fatal`).
    *   `format`: Log format (`json` or `text`).
*   **`postgres`:**
    *   `host`: PostgreSQL host.
    *   `port`: PostgreSQL port.
    *   `user`: PostgreSQL user.
    *   `password`: PostgreSQL password.
    *   `dbname`: PostgreSQL database name.
    *   `replication_slot`: Name of the PostgreSQL replication slot.
    *   `publication_name`: Name of the PostgreSQL publication.
*   **`broker`:**
    *   `type`: Broker type (`pubsub`, `nats`, `rabbitmq`, `inmemory`).
    *   `pubsub` (if `type: pubsub`):
        *   `project_id`: Google Cloud project ID.
        *   `topic_id`: Pub/Sub topic ID.
    *   `nats` (if `type: nats`):
        *   `url`: NATS server URL.
        *   `subject`: NATS subject to publish to.
    *   `rabbitmq` (if `type: rabbitmq`):
        *   `host`: RabbitMQ host.
        *   `port`: RabbitMQ port.
        *   `user`: RabbitMQ username.
        *   `password`: RabbitMQ password.
        *   `vhost`: RabbitMQ virtual host.
        *   `exchange`: RabbitMQ exchange name.
        *   `routing_key`: RabbitMQ routing key.
        *   `exchange_type`: RabbitMQ exchange type (`direct`, `fanout`, `topic`, `headers`).
        *   `durable`: Whether the exchange is durable.
        *   `auto_delete`: Whether the exchange is auto-deleted.
*   **`bigquery` (optional):**
    *   `project_id`: Google Cloud project ID.
    *   `dataset_id`: BigQuery dataset ID.
    *   `enable`: Whether to enable BigQuery integration (true/false).
    *   `tables`: A list of table mappings:
        *   `source`: Source table name (e.g., `public.users`).
        *   `target`: Target BigQuery table name.
        *   `primary_key`: Primary key column of the source table.
* **`initial_sync`:**
    * `enable`: Whether to enable initial synchronization (true/false).
    * `mode`: Synchronization mode (`snapshot` or `none`).

## `walstreamerctl` CLI

The `walstreamerctl` command-line tool provides a unified interface for managing Walstreamer.

### Commands

*   **`walstreamerctl stream`**: Starts the WAL streaming process.
*   **`walstreamerctl onboarding`**:  Sets up the necessary Google Cloud resources (Pub/Sub topics, subscriptions, BigQuery datasets, and tables).  This command automates the provisioning of these resources based on your `config.yaml` file.
*   **`walstreamerctl cleanup`**:  Cleans up the Google Cloud resources created by Walstreamer.  This command provides options for dry runs, interactive confirmation, and forced deletion.

### Global Flag

*   **`-c`, `--config`**: Specifies the path to the configuration file (default: `config.yaml`).  This flag is persistent and applies to all subcommands.  Example: `walstreamerctl stream -c myconfig.yaml`

### `stream` Command

Starts the WAL streaming process.  It reads the configuration file, connects to the PostgreSQL database, and begins streaming WAL data to the configured broker.

**Usage:**

```bash
walstreamerctl stream [-c <config_file>]
```

**Example:**

```bash
walstreamerctl stream -c config.yaml
```

This command will:

1.  Load the configuration from `config.yaml`.
2.  Connect to the PostgreSQL database using the specified credentials.
3.  Start reading WAL data from the specified replication slot.
4.  Decode the WAL data.
5.  Publish the decoded messages to the configured broker (Pub/Sub, NATS, RabbitMQ, or in-memory).
6.  Continuously stream data until interrupted (e.g., by Ctrl+C).
7.  Handle graceful shutdown on SIGINT and SIGTERM signals.

### `onboarding` Command

Creates or updates the necessary Google Cloud resources for Walstreamer.  This includes:

*   **Pub/Sub:** Creates the specified Pub/Sub topic and subscription if they don't exist.
*   **BigQuery:** Creates the specified BigQuery dataset and tables if they don't exist.  The table schema is automatically generated based on the PostgreSQL table definition and the `bigquery.tables` mapping in the configuration file.

**Usage:**

```bash
walstreamerctl onboarding [-c <config_file>]
```

**Example:**

```bash
walstreamerctl onboarding -c config.yaml
```

This command will:

1.  Load the configuration from `config.yaml`.
2.  Create or update the Pub/Sub topic and subscription.
3.  Create or update the BigQuery dataset and tables.

### `cleanup` Command

Removes the Google Cloud resources created by Walstreamer.  This helps to avoid unnecessary costs and keeps your Google Cloud environment clean.

**Usage:**

```bash
walstreamerctl cleanup [-c <config_file>] [--dry-run] [--force] [--interactive]
```

**Flags:**

*   **`--dry-run`**: Performs a dry run, showing which resources would be deleted without actually deleting them.
*   **`--force`**:  Deletes resources without asking for confirmation.  Use with caution!
*   **`--interactive`**:  Prompts for confirmation before deleting each resource.

**Examples:**

*   **Dry run:**

    ```bash
    walstreamerctl cleanup --dry-run -c config.yaml
    ```

*   **Interactive cleanup:**

    ```bash
    walstreamerctl cleanup --interactive -c config.yaml
    ```

*   **Forced cleanup (use with caution):**

    ```bash
    walstreamerctl cleanup --force -c config.yaml
    ```

This command will:

1.  Load the configuration from `config.yaml`.
2.  Identify the Google Cloud resources (Pub/Sub topic, subscription, BigQuery dataset, and tables) associated with the Walstreamer configuration.
3.  Depending on the flags provided:
    *   **`--dry-run`**: List the resources that would be deleted.
    *   **`--interactive`**: Prompt for confirmation before deleting each resource.
    *   **`--force`**: Delete the resources without confirmation.
    *   (No flags): Delete resources after a single confirmation.
4. Delete the identified resources.

## Initial Synchronization

Walstreamer supports initial data synchronization (snapshotting) to ensure data consistency between the source PostgreSQL database and the target system (e.g., BigQuery).  This is particularly useful when starting to stream data from an existing database.

To enable initial synchronization, set `initial_sync.enable` to `true` and `initial_sync.mode` to `snapshot` in your `config.yaml` file.  When enabled, Walstreamer will:

1.  Take a snapshot of the data in the tables specified in your configuration.
2.  Publish this snapshot data to the configured broker.
3.  Begin streaming WAL data from the point after the snapshot was taken.

This ensures that the target system receives a complete and consistent copy of the data before receiving real-time updates.

## Error Handling and Resilience

Walstreamer is designed to be resilient to common issues:

*   **Connection Interruptions:**  It automatically attempts to reconnect to the PostgreSQL database if the connection is lost.
*   **Panic Recovery:** The `stream` command includes panic recovery. If a panic occurs during streaming, it will be logged, and the process will exit gracefully.
*   **Broker Errors:**  Walstreamer handles errors that may occur when publishing messages to the broker (e.g., temporary network issues). The specific error handling behavior depends on the broker implementation.

## Building and Running

1.  **Clone the repository:**

    ```bash
    git clone https://repo.nusatek.id/sugeng/walstreamer.git
    cd walstreamer
    ```

2.  **Build the `walstreamerctl` binary:**

    ```bash
    go build ./cmd/walstreamerctl
    ```

3.  **Create a `config.yaml` file:**  Configure Walstreamer as described in the "Configuration" section.

4.  **Run Walstreamer:**

    *   **To start streaming:**

        ```bash
        ./walstreamerctl stream -c config.yaml
        ```

    *   **To set up Google Cloud resources:**

        ```bash
        ./walstreamerctl onboarding -c config.yaml
        ```

    *   **To clean up Google Cloud resources:**

        ```bash
        ./walstreamerctl cleanup -c config.yaml
        ```

## Contributing

Contributions to Walstreamer are welcome!  Please feel free to submit pull requests or open issues on the repository.

## License

[Insert License Information Here - e.g., MIT License]
