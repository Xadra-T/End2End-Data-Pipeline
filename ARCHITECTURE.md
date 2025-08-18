# System Architecture

This document provides a detailed overview of the components and data flow within this platform.

## High-Level Data Flow

The system operates in two main phases: a real-time ingestion path and a periodic batch analysis path.

```mermaid
graph TD
    subgraph "Real-time Ingestion"
        Producer[Producer Service] -->|Produce Avro Serialized Events| Kafka(Topic: user-interactions)
        Producer -->|Registers Schema| SchemaRegistry[Schema Registry]
        Kafka -->|Consume| KafkaConnect[Kafka Connect Sink]
        KafkaConnect -->|Batched Writes| ClickHouse(ClickHouse)
        KafkaConnect -->|Fetches Schema| SchemaRegistry
    end

    subgraph "Batch Analysis & Reporting"
        AirflowScheduler[Airflow Scheduler] -- Triggers each minute --> AirflowDAG[ETAR DAG]
        AirflowDAG -- Task 1: Extract --> ClickHouse
        ClickHouse -- Data for previous minute --> AirflowWorker[Airflow Worker]
        AirflowWorker -- Task 1: Stream Parquet --> MinIO(MinIO)
        AirflowDAG -- Task 2: Analysis --> Spark[Spark]
        Spark -- Reads Parquet --> MinIO
        Spark -- Writes JSON result --> MinIO
        AirflowDAG -- Task 3: Reads JSON result --> MinIO
        AirflowDAG -- Task 3: Report --> DashboardAPI[Dashboard API]
        DashboardUI[Dashboard UI] -- Polls for report --> DashboardAPI
    end

    subgraph "Airflow Backend"
        AirflowAPIserver & AirflowScheduler & AirflowWorker -->|Metadata| Postgres(Postgres DB)
        AirflowWorker -->|Task Queue| Redis(Redis Broker)
    end
```

## Component Breakdown

### 1. Data Generation (`producer`)
-   **Purpose:** Simulates simplified user interaction events for an e-commerce platform.
-   **Technology:** Python, `confluent-kafka-python`.
-   **Key Logic:**
    -   Generates events.
    -   Uses an Avro schema (`user_event_schema.avsc`) for data contracts.
    -   Serializes message values with `AvroSerializer` which automatically registers schemas with the Schema Registry.
    -   Serializes message keys (`user_id`) with a custom UUID serializer.
    -   Runs as a multi-process application to generate a higher volume of data.

### 2. Ingestion & Streaming (`kafka`, `schema-registry`, `connect`)
-   **Kafka (`broker`):**
    -   Runs in KRaft mode.
-   **Schema Registry (`schema-registry`):**
    -   Stores and serves the Avro schemas.
    -   Ensures that data written to Kafka conforms to a known, versioned structure.
-   **Kafka Connect (`connect` & `connect-helper`):**
    -   Provides a scalable and reliable way to stream data between Kafka and other systems.
    -   The official ClickHouse Sink Connector is used to move data from the Kafka topic to the ClickHouse database.
    -   The configuration (`clickhouse_connector_configuration.json`) is dynamically populated with environment variables by the `connect-helper` service, which uses `envsubst`.
    -   It uses the `AvroConverter` to deserialize messages, validating them against the schema from the Schema Registry before writing to ClickHouse.

### 3. Data Warehouse (`clickhouse`)
-   **Purpose:** Stores the raw event stream.
-   **Technology:** ClickHouse.
-   **Key Features:**
    -   **Schema:** The table `user_interactions` is defined in `clickhouse_table_schema.sql`.
    -   **Partitioning:** Data is partitioned by `event_minute` (a `MATERIALIZED` column). This is critical for performance, as it allows Airflow to efficiently query only the data for a specific minute without scanning the entire table.
    -   **Engine:** Uses the `MergeTree` engine, which is optimized for high-volume writes and fast analytical queries.

### 4. Orchestration (`airflow`)
-   **Purpose:** Manages the periodic batch analysis pipeline.
-   **Technology:** Apache Airflow with the `CeleryExecutor`.
-   **Components:**
    -   `Postgres`: Stores Airflow metadata (DAG states, task instances, connections, etc.).
    -   `Redis`: Acts as the message broker for Celery, queuing tasks for workers.
-   **The `etar_pipeline` DAG:**
    1. **Extract:** Runs every minute. Queries ClickHouse for data from the *previous* minute.
    2. **Transform/Load:** If data exists, it's converted to a Pandas DataFrame, then to an Arrow Table, and finally written as a Parquet file to a temporary location.
    3. **Store:** The Parquet file is stream into a MinIO bucket. The object name is the timestamp (e.g., `2025-08-09_10-30.parquet`).
    4. **Analyze:** Triggers a `SparkSubmitOperator` task, passing the S3A path of the Parquet file as an argument.
    5. **Report:** After the Spark job completes, a final task fetches the JSON analysis result from MinIO and POSTs it to the Dashboard API.

### 5. Batch Processing (`spark`)
-   **Purpose:** Performs the analysis on the minutely data extracts.
-   **Technology:** Apache Spark, PySpark.
-   **Key Logic:**
    -   The Spark application (`spark.py`) is submitted by Airflow.
    -   It reads a single Parquet file from MinIO. The S3A connector configuration is passed directly from the Airflow DAG.
    -   It performs a simple aggregation: counts total events, total errors, and success/error counts per event type.
    -   The result is written back to MinIO as a JSON file (e.g., `2025-08-09_10-30.json`).
    -   Exits with code `0` on success or non-zero on failure, signaling the status back to the Airflow task instance.

### 6. Storage (`minio`)
-   **Purpose:** Acts as the intermediate storage layer between the "Extract" and "Analyze" stages.
-   **Technology:** MinIO (S3-compatible object storage).
-   **Usage:**
    -   Stores minutely data extracts in Parquet format.
    -   Stores the JSON analysis results from Spark.

### 7. Presentation (`dashboard`)
-   **Purpose:** Displays the latest analysis results to the user.
-   **Technology:** FastAPI, Streamlit.
-   **Architecture:**
    -   **`dashboard-api`:** A simple FastAPI application with a single in-memory deque to store the most recent report. It provides a `/report` endpoint for Airflow to POST results to and for the UI to GET results from.
    -   **`dashboard-ui`:** A Streamlit application that runs in a loop, periodically polling the `/report` endpoint of the API. When it receives a new report, it updates the displayed chart and statistics.
