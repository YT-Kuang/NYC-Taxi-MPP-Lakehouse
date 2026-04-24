# NYC-Taxi-MPP-Lakehouse

Local lakehouse analytics project for NYC yellow taxi data using AWS S3, AWS
Glue Catalog, Apache Iceberg, PySpark, Trino, dbt, Airflow, and Docker Compose.

This project demonstrates how a modern analytics platform can be assembled from
open table formats, SQL engines, batch processing, transformation tooling, and
orchestration while still being reproducible on a single developer machine. It
is intended for data engineers, analytics engineers, and technical stakeholders
who want a concrete reference for moving raw taxi data into bronze, silver,
staging, and gold analytical layers.

The repository is intended to be restorable on a fresh machine by cloning the
repo, configuring local AWS credentials, creating a `.env`, bootstrapping AWS
resources and raw data, starting the Docker stack, validating services, and
running the Airflow pipeline end to end.

## Quick Start

```bash
git clone <repo-url>
cd nyc-taxi-mpp-lakehouse
cp .env.example .env
# Edit .env, especially AWS_PROFILE, AWS_REGION, and S3_BUCKET.
make bootstrap
```

Important: `make bootstrap` creates or validates AWS resources and uploads local
raw/reference data to S3.

## Contents

- [Project Overview](#project-overview)
- [Architecture Summary](#architecture-summary)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Fresh-Machine Setup](#fresh-machine-setup)
- [AWS Configuration](#aws-configuration)
- [`.env` Configuration](#env-configuration)
- [Bootstrap vs Step-by-Step Setup](#bootstrap-vs-step-by-step-setup)
- [Build Images and Start Containers](#build-images-and-start-containers)
- [Upload and Bootstrap Raw Data](#upload-and-bootstrap-raw-data)
- [Validation](#validation)
- [Run the Pipeline End to End](#run-the-pipeline-end-to-end)
- [Inspect Data with Trino](#inspect-data-with-trino)
- [Inspect Data with DBeaver](#inspect-data-with-dbeaver)
- [Troubleshooting](#troubleshooting)
- [What Is Intentionally Not Committed](#what-is-intentionally-not-committed)

## Project Overview

This project builds a small local analytics lakehouse around NYC yellow taxi
trip data:

- Raw Parquet and reference CSV files live locally under `data/`.
- Setup scripts upload those files into S3 using the expected lakehouse layout.
- PySpark jobs read raw S3 data and write Iceberg bronze/silver tables through
  the Glue Catalog.
- dbt models run through Trino to create staging views and gold tables.
- Airflow orchestrates raw checks, Spark jobs, dbt parse/run/test, and task
  tests.
- Trino exposes the Iceberg catalog for SQL inspection and external clients
  such as DBeaver.

## Architecture Summary

```text
data/raw + data/reference
        |
        | scripts/upload_raw_data.sh
        v
AWS S3 raw prefixes
        |
        | Airflow DAG: Spark bronze + silver tasks
        v
Iceberg tables in Glue database: nyc_lakehouse
        |
        | Airflow DAG: dbt parse/run/test through Trino
        v
dbt staging views in Glue database: staging
dbt gold tables in Glue database: gold
        |
        | Trino
        v
SQL clients, Trino CLI, DBeaver
```

### Local Docker Services

- `trino`: Trino 480 on host port `8080`
- `airflow-postgres`: Airflow metadata database on host port `5434`
- `airflow-init`: one-shot Airflow DB/user initialization
- `airflow-webserver`: Airflow UI on host port `8081`
- `airflow-scheduler`: Airflow scheduler

## Repository Structure

```text
.
├── data/
│   ├── raw/                         # Local yellow_tripdata_YYYY-MM.parquet files
│   └── reference/                   # Local taxi_zone_lookup.csv
├── dbt/
│   ├── nyc_taxi_dbt/                # dbt project, models, tests, macros
│   └── profiles/profiles.yml        # Repo-owned dbt profile mounted into Airflow
├── infra/
│   ├── airflow/
│   │   ├── Dockerfile               # Airflow image with Java, Spark, dbt deps
│   │   ├── dags/                    # nyc_taxi_lakehouse DAG
│   │   └── requirements.txt         # Container Python dependencies
│   ├── trino/catalog/iceberg.properties
│   └── setup_bucket.sh              # Idempotent S3 bucket/prefix setup
├── scripts/
│   ├── 01_bronze_ingest.py          # Spark bronze ingest
│   ├── 02_silver_transform.py       # Spark silver transform
│   ├── bootstrap.sh                 # Full local bootstrap
│   ├── bootstrap_aws.sh             # AWS/S3/Glue bootstrap
│   ├── env.sh                       # Shared .env loader/defaults
│   ├── setup_local_python.sh        # Local .venv setup
│   ├── upload_raw_data.sh           # Upload local raw/reference files to S3
│   ├── validate_aws.sh              # AWS CLI/profile validation
│   └── validate_stack.sh            # Airflow/dbt/Trino validation
├── docker-compose.yml
├── Makefile
├── .env.example
└── README.md
```

## Prerequisites

Install these on the host machine before bootstrapping:

- Docker with Docker Compose
- Python 3.11+
- AWS CLI v2
- Local AWS credentials configured outside this repository
- Network access for Docker image pulls, Python package installs, and Spark
  Maven package resolution during Airflow tasks

The AWS identity needs permissions for:

- `sts:GetCallerIdentity`
- S3 bucket create/head/put/copy operations for the configured bucket
- Glue database get/create operations
- Glue Catalog and S3 access needed by Spark, Trino, and Iceberg

## Fresh-Machine Setup

Clone the repository and create local configuration:

```bash
git clone <repo-url>
cd nyc-taxi-mpp-lakehouse
cp .env.example .env
```

Edit `.env` before running bootstrap. At minimum, review:

```bash
AWS_PROFILE=<your-local-aws-profile>
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=<globally-unique-s3-bucket-name>
```

Then run the full bootstrap:

```bash
make bootstrap
```

Important: `make bootstrap` performs AWS mutations and uploads data. It creates
or validates S3 resources, creates or validates Glue databases, uploads local
raw/reference files to S3, builds Docker images, starts containers, and runs
stack validation.

## AWS Configuration

AWS credentials must live outside git. The default setup expects a named AWS
profile in the host AWS config directory:

```bash
aws configure --profile nyc-lakehouse
```

Validate the profile:

```bash
make validate-aws
```

By default, Docker mounts host AWS config from `${HOME}/.aws` into:

- `/home/airflow/.aws` for Airflow containers
- `/home/trino/.aws` for the Trino container

If your AWS config lives elsewhere, set `AWS_CONFIG_DIR` in `.env`:

```bash
AWS_CONFIG_DIR=/absolute/path/to/.aws
```

Do not commit `.env`, AWS credentials, access keys, session tokens, or local
machine-specific credential files.

## `.env` Configuration

`.env.example` documents the supported variables. The current defaults are:

```bash
AWS_PROFILE=nyc-lakehouse
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1

S3_BUCKET=nyc-taxi-mpp-lakehouse
RAW_PREFIX=raw/nyc_taxi/yellow_tripdata/
REFERENCE_PREFIX=raw/nyc_taxi/reference/
WAREHOUSE_PREFIX=warehouse/

TRINO_CATALOG=iceberg
GLUE_DATABASE_RAW=nyc_lakehouse
GLUE_DATABASE_STAGING=staging
GLUE_DATABASE_GOLD=gold
SPARK_CATALOG_NAME=demo
RAW_TRIP_MONTHS=2025-01,2025-02

AIRFLOW_UID=50000
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin

DBT_PROFILE_NAME=nyc_taxi_dbt
DBT_TARGET=dev

TRINO_HOST=trino
TRINO_PORT=8080
TRINO_USER=airflow
```

### Container vs Host Values

- `TRINO_HOST=trino` is for dbt running inside Docker, where `trino` is the
  Compose service name.
- From the host machine, use `localhost:8080` for Trino.
- From the host machine, use `localhost:8081` for Airflow.
- Inside Airflow containers, the project is mounted at `/opt/nyc_taxi_lakehouse`.
- The repo-owned dbt profile is `dbt/profiles/profiles.yml` and is mounted at
  `/home/airflow/.dbt`.

Use a globally unique `S3_BUCKET`. The default is a template-friendly value, but
S3 bucket names are global and may already exist in another AWS account.

## Bootstrap vs Step-by-Step Setup

### Full Bootstrap

```bash
make bootstrap
```

This runs:

```bash
./scripts/setup_local_python.sh
./scripts/bootstrap_aws.sh
./scripts/upload_raw_data.sh
docker compose up -d --build
./scripts/validate_stack.sh
```

### Step-by-Step Setup

```bash
make setup-python
make validate-aws
make bootstrap-aws
make upload-raw
make up
make validate
```

Use the step-by-step path when you want to inspect AWS resources before upload,
debug credentials, or avoid starting Docker until S3/Glue setup is complete.

## Build Images and Start Containers

Build and start the stack:

```bash
make up
```

Equivalent command:

```bash
docker compose up -d --build
```

Inspect services:

```bash
make ps
docker compose ps
```

Follow logs:

```bash
make logs
docker compose logs --tail=200
```

Restart key services:

```bash
make restart
```

Stop the stack:

```bash
make down
```

Airflow UI:

```text
http://localhost:8081
```

Default Airflow credentials come from `.env`:

```text
admin / admin
```

Trino host endpoint:

```text
http://localhost:8080
```

## Upload and Bootstrap Raw Data

Local source files:

```text
data/raw/yellow_tripdata_2025-01.parquet
data/raw/yellow_tripdata_2025-02.parquet
data/raw/yellow_tripdata_2025-03.parquet
data/raw/yellow_tripdata_2025-04.parquet
data/raw/yellow_tripdata_2025-05.parquet
data/raw/yellow_tripdata_2025-06.parquet
data/reference/taxi_zone_lookup.csv
```

Create/validate S3 bucket prefixes and Glue databases:

```bash
make bootstrap-aws
```

Upload local raw/reference data:

```bash
make upload-raw
```

`scripts/upload_raw_data.sh` uploads Parquet files to:

```text
s3://$S3_BUCKET/$RAW_PREFIX/yearYYYY/yellow_tripdata_YYYY-MM.parquet
```

It uploads the taxi zone reference file to:

```text
s3://$S3_BUCKET/$REFERENCE_PREFIX/taxi_zone_lookup.csv
```

The Spark bronze job currently reads the months listed in `RAW_TRIP_MONTHS`.
The default is:

```bash
RAW_TRIP_MONTHS=2025-01,2025-02
```

The default only ingests January and February 2025 to keep the first end-to-end
run smaller and easier to validate on a local Airflow/Spark setup. The repository
contains additional 2025 Parquet files, and `make upload-raw` uploads them, but
the pipeline only ingests the months configured in `RAW_TRIP_MONTHS`. To process
more months, update `RAW_TRIP_MONTHS` in `.env` and rerun the pipeline.

## Validation

### AWS Dependency Validation

```bash
make validate-aws
```

This checks that the AWS CLI exists, `AWS_CONFIG_DIR` exists, and
`aws sts get-caller-identity` succeeds for `AWS_PROFILE` and `AWS_REGION`.

### Stack Validation

```bash
make validate
```

This runs:

```bash
docker compose ps
docker exec airflow-webserver airflow dags list
docker exec airflow-webserver bash -lc "cd /opt/nyc_taxi_lakehouse/dbt/nyc_taxi_dbt && dbt debug --profiles-dir /home/airflow/.dbt"
docker exec trino trino --execute "SHOW CATALOGS"
```

### Individual Service Checks

```bash
make ps
make dbt-debug
make trino-cli
docker exec -it airflow-webserver airflow dags list
docker exec -it trino trino
```

### Airflow Task Smoke Tests

```bash
make test-raw
make test-bronze
make test-silver
make test-dbt-parse
make test-dbt-run
make test-dbt-test
```

These use Airflow's `tasks test` command with the fixed logical date
`2026-04-17`, as defined in the `Makefile`.

### Spark Validation

- `bronze_ingest` runs `scripts/01_bronze_ingest.py`
- `silver_transform` runs `scripts/02_silver_transform.py`

Both tasks use `spark-submit` inside the Airflow image and resolve Iceberg/AWS
Spark packages through the `--packages` argument.

## Run the Pipeline End to End

Start the stack and validate it:

```bash
make up
make validate
```

Trigger the DAG:

```bash
make trigger-dag
```

The DAG id is:

```text
nyc_taxi_lakehouse
```

Pipeline task order:

```text
check_raw_data_exists
  -> bronze_ingest
  -> silver_transform
  -> dbt_parse
  -> dbt_run
  -> dbt_test
```

You can monitor the run in Airflow at:

```text
http://localhost:8081
```

## Inspect Data with Trino

Open the Trino CLI inside the Trino container:

```bash
make trino-cli
```

Example SQL:

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.nyc_lakehouse;
SHOW TABLES FROM iceberg.staging;
SHOW TABLES FROM iceberg.gold;

SELECT count(*) FROM iceberg.nyc_lakehouse.bronze_yellow_trip_raw;
SELECT count(*) FROM iceberg.nyc_lakehouse.silver_trip_enriched;
SELECT * FROM iceberg.gold.fact_trip LIMIT 10;
```

The catalog name comes from:

```bash
TRINO_CATALOG=iceberg
```

## Inspect Data with DBeaver

Create a Trino connection in DBeaver from the host machine:

- Driver: Trino
- Host: `localhost`
- Port: `8080`
- Authentication: no authentication
- User: any non-empty value, for example `airflow`
- Catalog: `iceberg`

Use `localhost:8080` from DBeaver because DBeaver runs on the host machine. Do
not use `trino:8080` there; `trino` is only the Docker Compose service hostname
available to other containers.

After connecting, browse schemas:

- `nyc_lakehouse` for Spark bronze/silver Iceberg tables
- `staging` for dbt staging views
- `gold` for dbt marts

### Host and Container Connection Names

- DBeaver runs on the host, so use `localhost:8080`.
- dbt runs in Airflow containers, so it uses `TRINO_HOST=trino`.

## Troubleshooting

### AWS Profile Not Found

```bash
make validate-aws
```

Confirm `AWS_PROFILE` in `.env` matches a local profile:

```bash
aws configure list-profiles
aws sts get-caller-identity --profile "$AWS_PROFILE" --region "$AWS_REGION"
```

### S3 Bucket Creation Fails

- S3 bucket names are global. Change `S3_BUCKET` in `.env`.
- Confirm the AWS identity has permission to create or access the bucket.
- Rerun `make bootstrap-aws`.

### Airflow Cannot See the dbt Profile

- Confirm `docker-compose.yml` mounts `./dbt/profiles` to `/home/airflow/.dbt`.
- Run `make dbt-debug`.
- Check that `.env` contains `TRINO_HOST=trino`, not `localhost`, for container
  dbt execution.

### Trino Cannot Access S3 or Glue

- Confirm `${AWS_CONFIG_DIR:-${HOME}/.aws}` exists on the host.
- Confirm the selected AWS profile works from the host.
- Confirm Trino has `AWS_REGION` in the container environment.
- Restart Trino after `.env` changes:

```bash
docker compose restart trino
```

### Spark Task Fails While Resolving Packages

- Airflow Spark tasks use `spark-submit --packages`.
- Confirm the Airflow container has network access to Maven repositories.
- Check Airflow task logs for dependency resolution errors.

### Raw Check Fails

- Run `make upload-raw`.
- Confirm objects exist under:

```text
s3://$S3_BUCKET/$RAW_PREFIX
```

### Bronze Reads No Files or Wrong Files

- Confirm `RAW_TRIP_MONTHS` matches uploaded files.
- The expected S3 layout includes a `yearYYYY` directory.

### Port Conflicts

- Airflow webserver uses host port `8081`.
- Trino uses host port `8080`.
- Airflow Postgres uses host port `5434`.
- Change the published ports in `docker-compose.yml` if another local service
  already uses them.

### Benchmarking SQL Changes

- Do not claim a measured performance improvement unless it is benchmarked.
- A statement like "target a 25% SQL runtime improvement" is acceptable only as
  a benchmark goal.
- To measure it properly, capture baseline and candidate query runtimes using
  the same input data, same warehouse state, same Trino settings, repeated runs,
  and recorded query text/results.

## What Is Intentionally Not Committed

The repository should not commit local or generated runtime state:

- `.env`
- AWS credentials or config files
- `.aws/`
- `.venv/`
- Airflow logs under `infra/airflow/logs/`
- dbt logs and compiled artifacts under `dbt/nyc_taxi_dbt/logs/` and
  `dbt/nyc_taxi_dbt/target/`
- Python `__pycache__/` and `*.pyc`
- `.DS_Store`
- editor folders such as `.vscode/`
- temporary files under `tmp/`

The repo intentionally includes local raw/reference data under `data/` so a
fresh clone has enough source data for `make upload-raw` and `make bootstrap` to
populate S3 without requiring a separate download step. That choice makes the
fresh-machine workflow simpler and more reliable, but it also increases the
repository size because Parquet files are committed. If this data strategy
changes, update this README and the upload scripts together.
