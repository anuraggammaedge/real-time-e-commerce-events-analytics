# E-Commerce Real-Time Streaming Analytics Platform

## Overview

This project demonstrates an end-to-end real-time streaming data platform built using Apache Kafka, Apache Spark Structured Streaming, Delta Lake, PostgreSQL, and Apache Superset.

The pipeline continuously generates e-commerce events, processes them through multiple streaming layers, aggregates business metrics, stores them in an analytical database, and visualizes them through dashboards.

---

# Architecture

```
                    +----------------------+
                    |  Event Producer      |
                    |  (Python)            |
                    +----------+-----------+
                               |
                               |
                               v
                     +------------------+
                     | Apache Kafka     |
                     | Topic: events    |
                     +---------+--------+
                               |
                               |
                               v
                     +------------------+
                     | Schema Registry  |
                     | (Avro Schema)    |
                     +---------+--------+
                               |
                               |
                               v
               +-------------------------------+
               | Spark Structured Streaming    |
               | Raw Ingestion Job             |
               +---------------+---------------+
                               |
                               |
                               v
                      Bronze Delta Lake
                   (Raw immutable events)
                               |
                               |
                               v
               +-------------------------------+
               | Spark Silver Streaming Job    |
               | Data Cleaning & Validation    |
               +---------------+---------------+
                               |
                               |
                               v
                      Silver Delta Lake
                   (Cleaned business events)
                               |
               +---------------+---------------+
               |                               |
               |                               |
               v                               v
    Gold Aggregation Job             Gold Aggregation Job
  Events Per Minute                 Category Metrics
               |                               |
               +---------------+---------------+
                               |
                               |
                               v
                     Gold Delta Lake Tables
               • gold_events_per_minute
               • gold_category_metrics
                               |
                               |
                               v
                    PostgreSQL Analytics DB
                               |
                               |
                               v
                    Apache Superset Dashboard
```

---

# Technology Stack

| Layer              | Technology                        |
| ------------------ | --------------------------------- |
| Event Generator    | Python                            |
| Streaming Platform | Apache Kafka                      |
| Schema Management  | Confluent Schema Registry         |
| Stream Processing  | Apache Spark Structured Streaming |
| Data Lake          | Delta Lake                        |
| Analytics Database | PostgreSQL                        |
| Dashboard          | Apache Superset                   |
| Containerization   | Docker Compose                    |

---

# Folder Structure

```
ecommerce-streaming/
│
├── analytics/
│   ├── loader/
│   │   └── load_postgres.py
│   ├── sql/
│   ├── verify.py
│   └── verify_postgres.py
│
├── checkpoints/
│   ├── raw_events/
│   ├── silver_events/
│   ├── gold_events_per_minute/
│   └── gold_category_metrics/
│
├── delta/
│   ├── raw_events/
│   ├── silver_events/
│   ├── gold_events_per_minute/
│   └── gold_category_metrics/
│
├── src/
│   ├── producer/
│   ├── kafka/
│   ├── spark/
│   └── model/
│
├── superset/
│   └── Dockerfile
│
├── docker-compose.yml
└── README.md
```

---

# Data Pipeline

## Step 1 — Event Producer

The producer continuously generates synthetic e-commerce events.

Example event:

```json
{
    "event_id": "...",
    "user_id": 5012,
    "product_id": 102,
    "category": "electronics",
    "event_timestamp": "2026-06-26T12:10:05Z"
}
```

The producer serializes the event using Avro and publishes it to Kafka.

---

## Step 2 — Kafka

Kafka acts as the streaming backbone.

Responsibilities:

* Durable event storage
* High throughput
* Partitioning
* Scalability
* Consumer groups

---

## Step 3 — Schema Registry

Stores the Avro schema separately from the data.

Benefits:

* Schema evolution
* Compatibility checks
* Smaller Kafka messages
* Strong typing

---

## Step 4 — Bronze Layer

Spark consumes Kafka messages.

Bronze layer stores:

* Raw events
* Immutable records
* Original schema

No business transformations are applied.

---

## Step 5 — Silver Layer

Spark validates and cleans the raw events.

Operations include:

* Removing null values
* Filtering invalid events
* Selecting required columns
* Timestamp parsing

Silver data becomes the trusted source for analytics.

---

## Step 6 — Gold Layer

Business aggregations are computed.

### Events Per Minute

Calculates the number of events generated every minute.

Output:

| window_start | window_end | count |
| ------------ | ---------- | ----- |

---

### Category Metrics

Calculates events grouped by:

* Time window
* Category

Output:

| window_start | category | count |
| ------------ | -------- | ----- |

---

# PostgreSQL Analytics Layer

Delta tables are periodically loaded into PostgreSQL.

Current tables:

```
events_per_minute
category_metrics
```

PostgreSQL serves as the analytical database queried by Apache Superset.

---

# Dashboard Layer

Apache Superset connects directly to PostgreSQL.

Current dashboards include:

* Events Per Minute
* Category Metrics
* Time Series
* Category Distribution
* Business KPIs

---

# Data Flow

```
Producer
    │
    ▼
Kafka
    │
    ▼
Spark Raw Job
    │
    ▼
Bronze Delta
    │
    ▼
Spark Silver Job
    │
    ▼
Silver Delta
    │
    ▼
Gold Aggregation Jobs
    │
    ▼
Gold Delta Tables
    │
    ▼
PostgreSQL
    │
    ▼
Apache Superset
```

---

# Running the Project

## 1. Start Infrastructure

```bash
docker compose up -d
```

Starts:

* Kafka
* Zookeeper
* Schema Registry
* PostgreSQL
* Apache Superset

---

## 2. Start Producer

```bash
python src/producer/producer.py
```

---

## 3. Start Spark Jobs

### Raw Ingestion

```bash
python src/spark/raw_ingestion.py
```

### Silver Layer

```bash
python src/spark/silver.py
```

### Gold Events Per Minute

```bash
python src/spark/gold_events_per_minute.py
```

### Gold Category Metrics

```bash
python src/spark/gold_category_metrics.py
```

---

## 4. Load Gold Data into PostgreSQL

```bash
python analytics/loader/load_postgres.py
```

---

## 5. Open Superset

```
http://localhost:8088
```

Login:

```
Username: admin
Password: admin
```

---

# Current Pipeline Characteristics

* Near real-time event processing
* Streaming ETL
* Medallion Architecture (Bronze → Silver → Gold)
* Delta Lake storage
* PostgreSQL analytical serving layer
* Interactive dashboards with Superset
* Docker-based local deployment

---

# Future Improvements

* Apache Airflow for orchestration
* Continuous Delta → PostgreSQL synchronization
* Spark checkpoints on object storage
* Partitioned Delta tables
* CI/CD pipeline
* Kubernetes deployment
* Prometheus + Grafana monitoring
* dbt for SQL transformations
* Great Expectations for data quality
* Alerting for failed streaming jobs

---

# Learning Outcomes

This project demonstrates practical experience with:

* Apache Kafka
* Schema Registry
* Apache Spark Structured Streaming
* Delta Lake
* Medallion Architecture
* Streaming ETL
* PostgreSQL
* Apache Superset
* Docker
* Real-time Analytics
* Data Engineering Pipeline Design
