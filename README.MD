# Insignia Flows

This project is a data engineering and orchestration playground built around Apache Airflow, Apache Spark, Oracle, MinIO, Trino, and Iceberg. It is designed to demonstrate how a batch data pipeline can ingest source tables, transform them, and publish a snapshot layer for downstream analytics and reporting.

The current repository is intentionally a working baseline rather than a production-ready deployment. It includes working infrastructure definitions, DAG templates, and sample data loading patterns, but several pieces are meant to be adapted to your own business rules, schema, and data sensitivity requirements.

> Important: the current seed scripts and CSVs are examples/templates and should not be treated as production-ready data definitions. The included files use locally available sample data and template structures only because the real sensitive data has been removed or replaced.

---

## 1. Project purpose

The repository combines:

- Airflow for orchestration and scheduling
- Spark for transformation and snapshot logic
- Oracle as the source/bronze system
- MinIO + Iceberg for storage and lakehouse table management
- Trino for query-layer access
- PostgreSQL for Airflow metadata

The overall pattern is:

1. Source data is loaded into Oracle bronze tables.
2. Airflow schedules internal pipeline tasks.
3. Spark jobs read Oracle data and produce snapshot-style aggregated data.
4. The result is written to Iceberg tables in MinIO.
5. Downstream analytical consumers can query the lakehouse using Trino.

---

## 2. Architecture overview

### Core stack

- Airflow 3 with LocalExecutor
- PostgreSQL backend for Airflow metadata
- Spark container for batch processing
- Iceberg REST catalog with MinIO object storage
- Trino for querying Iceberg tables
- Oracle database for example source data
- MinIO for warehouse object storage

### Main services in docker-compose

The project provisions these major services:

- `airflow-api-server`
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-triggerer`
- `airflow-init`
- `airflow-cli`
- `postgres`
- `oracle`
- `spark-iceberg`
- `rest` (Iceberg REST catalog)
- `minio`
- `mc` (MinIO client for bucket bootstrap)
- `trino`

The file [docker-compose.yml](docker-compose.yml) is the central runtime definition. It wires the data platform together and mounts the project folders into the Airflow and Spark containers.

---

## 3. Repository structure

```text
insignia-flows/
├── airflow_projects/
│   ├── dags/
│   │   ├── credit_score-pipe.py
│   │   └── snapshot-pipe.py
│   └── scripts/
│       └── main_pipeline/
│           ├── helper.py
│           └── pyscripts/
│               └── snapshot-job.py
├── blueprint/
│   ├── oracle/
│   │   ├── seed/
│   │   │   ├── 00-user.sql
│   │   │   ├── 00-user.sql.template
│   │   │   ├── 01-customers.ctl
│   │   │   ├── 02-accounts.ctl
│   │   │   ├── 03-transactions.ctl
│   │   │   ├── 04-credit_scores.ctl
│   │   │   ├── 05-app_events.ctl
│   │   │   ├── 06-support_tickets.ctl
│   │   │   ├── *.csv
│   │   └── oracle.Dockerfile
│   ├── airflow.Dockerfile
│   ├── airflow.requirements.txt
│   ├── spark.Dockerfile
│   └── spark-defaults.conf
├── airflow_data/
├── minio_data/
├── trino/
├── oracle-tmp/
├── .env.template
├── docker-compose.yml
├── requirements.txt
└── Readme.MD
```

---

## 4. DAGs and orchestration logic

### 4.1 main-pipeline

File: [airflow_projects/dags/snapshot-pipe.py](airflow_projects/dags/snapshot-pipe.py)

This is the main production-style DAG in the project.

What it does:

- Starts with an `EmptyOperator`
- Runs a Spark job through `SparkSubmitOperator`
- Invokes the application script at `/opt/airflow/projects/scripts/main_pipeline/pyscripts/snapshot-job.py`
- Passes the Airflow execution date as `--run_date`
- Triggers the secondary DAG `credit-score-pipeline` after success

Key details:

- Schedule: `0 0 1 * *` (first day of each month)
- `catchup=True`
- `start_date=datetime(2020, 5, 16)`
- Spark job uses `spark_default` connection
- It writes into the `silver.account_snapshot` Iceberg table
- On failure it invokes the email alert helper

This DAG is a practical example of a batch pipeline that creates a point-in-time account snapshot for analytics.

### 4.2 credit-score-pipeline

File: [airflow_projects/dags/credit_score-pipe.py](airflow_projects/dags/credit_score-pipe.py)

This DAG is much simpler and appears to be a lightweight placeholder/demo pipeline.

What it does:

- Starts with `EmptyOperator`
- Runs a Python task called `calculate_score`
- Executes a simple function that prints a success message
- Ends after the task

This DAG is useful as a template for a downstream scoring or enrichment step. It is not yet tied to a real credit model or a production scoring workflow.

---

## 5. Spark snapshot logic

File: [airflow_projects/scripts/main_pipeline/pyscripts/snapshot-job.py](airflow_projects/scripts/main_pipeline/pyscripts/snapshot-job.py)

This is the most important processing script in the repository.

It does the following:

- Reads credentials from environment variables (`ORACLE_USERNAME`, `ORACLE_PASSWORD`, `ORACLE_HOST`, `ORACLE_SERVICE`)
- Creates a Spark session
- Verifies whether the Iceberg table `silver.account_snapshot` exists
- Reads the previous day snapshot from Iceberg
- Pulls account master data from the Oracle `ACCOUNTS` table
- Pulls completed transactions for the current date from `TRANSACTIONS`
- Joins and aggregates transaction amounts by account
- Computes a rolling snapshot with:
  - `account_id`
  - `customer_id`
  - `account_type`
  - `balance`
  - `sum_amount`
  - `total_transactions`
  - `snapshot_date`
  - `last_updated`
- Writes the result back to the Iceberg table using partitioning on `snapshot_date`

This is a classic incremental daily snapshot pattern and is a good example of how to carry state forward from one day to the next.

---

## 6. Oracle seed and schema setup

The initial Oracle schema is defined in [blueprint/oracle/seed/00-user.sql](blueprint/oracle/seed/00-user.sql).

This file creates the `bronze` user and the following tables:

- `bronze.customers`
- `bronze.accounts`
- `bronze.transactions`
- `bronze.credit_scores`
- `bronze.app_events`
- `bronze.support_tickets`

The schema includes:

- primary keys
- foreign keys
- data type constraints
- check constraints on statuses, categories, and segments

### Seed templates and loaders

The loader templates live in the same folder:

- [blueprint/oracle/seed/01-customers.ctl](blueprint/oracle/seed/01-customers.ctl)
- [blueprint/oracle/seed/02-accounts.ctl](blueprint/oracle/seed/02-accounts.ctl)
- [blueprint/oracle/seed/03-transactions.ctl](blueprint/oracle/seed/03-transactions.ctl)
- [blueprint/oracle/seed/04-credit_scores.ctl](blueprint/oracle/seed/04-credit_scores.ctl)
- [blueprint/oracle/seed/05-app_events.ctl](blueprint/oracle/seed/05-app_events.ctl)
- [blueprint/oracle/seed/06-support_tickets.ctl](blueprint/oracle/seed/06-support_tickets.ctl)

These are SQL*Loader control files used to load CSVs into Oracle. They are examples of how to map flat data into target tables.

The `*.csv` files under the same directory are the actual source data used by the template loading strategy.

> This is the most important customization point in the project. The current data is a template-style sample and is not meant to be used as-is for sensitive production data. Replace the seed, CSVs, SQL schemas, and CTL mapping files to match your own domain and data policy.

---

## 7. Sensitive data and security note

This repository currently contains example data and seed templates rather than production or customer-sensitive datasets.

Please treat the following as required review items before using this in a real environment:

- Replace or remove all local CSV seed files with your own approved data sources
- Update the Oracle schema in [blueprint/oracle/seed/00-user.sql](blueprint/oracle/seed/00-user.sql) to fit your business model
- Update the CTL files to match the actual columns, formats, and delimiters used by your data files
- Do not commit personal, financial, or customer-identifying data to the repository
- Rotate any default or placeholder credentials in the environment file
- Review the email notification helper in [airflow_projects/scripts/main_pipeline/helper.py](airflow_projects/scripts/main_pipeline/helper.py) because it currently sends to a placeholder address

> The project is intentionally built around template-driven and locally available sample files, but the real-world implementation should be adapted to your own data, policies, and governance standards.

---

## 8. Environment configuration

Use [.env.template](.env.template) as the base for your local environment file.

Create a `.env` file from the template and fill in the required values:

- MinIO credentials
- Oracle credentials and host
- PostgreSQL credentials for Airflow
- Optional Airflow username/password

Example values are included in [.env.template](.env.template), but you need to replace them with your own secure values before starting the stack.

---

## 9. Quick start

### Prerequisites

- Docker and Docker Compose
- Enough disk and memory for the stack
- Linux environment recommended for this project layout
- Access to the repo root and proper local permissions

### Start the platform

From the project root:

```bash
docker compose up -d
```

If you need the MinIO initialization profile as well:

```bash
docker compose --profile init up -d
```

Then check the Airflow UI and the service endpoints once the containers are running.

---

## 10. Customization checklist

Before using this project in production or for a real business dataset, review and update the following:

1. Environment values in [.env.template](.env.template)
2. Oracle user and table definitions in [blueprint/oracle/seed/00-user.sql](blueprint/oracle/seed/00-user.sql)
3. Oracle loader mappings in all CTL files under [blueprint/oracle/seed](blueprint/oracle/seed)
4. Spark SQL queries in [airflow_projects/scripts/main_pipeline/pyscripts/snapshot-job.py](airflow_projects/scripts/main_pipeline/pyscripts/snapshot-job.py)
5. Airflow scheduling and trigger logic in [airflow_projects/dags/snapshot-pipe.py](airflow_projects/dags/snapshot-pipe.py)
6. Email alert recipients in [airflow_projects/scripts/main_pipeline/helper.py](airflow_projects/scripts/main_pipeline/helper.py)
7. Data retention, partitioning, and lakehouse naming conventions

---

## 11. What this project is good for

This repository is useful for:

- learning Airflow + Spark orchestration patterns
- prototyping lakehouse pipelines with Iceberg and MinIO
- testing daily snapshot logic over source systems
- creating a small data platform stack locally in Docker
- understanding how to structure data engineering components around batch jobs

It is not intended to be a drop-in production system without custom validation and domain-specific tuning.

---

## 12. Recommended next steps

- Replace placeholder seed files with your own schema and data contract
- Model actual business rules in the SQL and Spark queries
- Add quality checks and validation steps in Airflow
- Expand the pipeline with bronze/silver/gold or medallion-layer conventions
- Add monitoring, alerting, and lineage tracking for production use

---

## 13. Final note

This project is a technical baseline that demonstrates the shape of a modern batch data platform. It is intentionally easy to understand, but the parts that actually represent business logic and sensitive data must be reviewed and adjusted before they are used outside of a local or demo environment.

If you plan to use this repository as a real implementation, the main rule is simple:

- adapt the seed,
- adapt the SQL,
- adapt the CTL templates,
- adapt the pipeline logic,
- and protect sensitive data from being committed into the repository.
