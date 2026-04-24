# NYC-Taxi-MPP-Lakehouse

Local lakehouse analytics project for NYC yellow taxi data using S3, Glue
Catalog, Iceberg, PySpark, Trino, dbt, Airflow, and Docker Compose.

## Fresh machine setup

Prerequisites:

- Docker with Docker Compose
- Python 3.11+
- AWS CLI v2
- Local AWS credentials configured outside this repository

Create local configuration:

```bash
cp .env.example .env
```

Edit `.env` for your AWS profile, region, and S3 bucket. The bucket name must
be globally unique. Do not put AWS secrets in `.env`.

Bootstrap the project:

```bash
make bootstrap
```

That command creates a local Python environment, validates AWS credentials,
creates or validates S3 and Glue resources, uploads the repo's raw data to the
expected S3 layout, builds Docker images, starts services, and validates the
stack.

## Useful commands

```bash
make setup-python    # create .venv and install local Python dependencies
make validate-aws    # validate AWS CLI credentials/profile
make bootstrap-aws   # create/validate S3 bucket and Glue databases
make upload-raw      # upload data/raw and data/reference to S3
make up              # build and start Docker services
make validate        # validate Airflow, dbt, and Trino
make trigger-dag     # trigger the full Airflow DAG
```

Airflow UI: <http://localhost:8081>

Trino endpoint from the host: <http://localhost:8080>

## Configuration

The main configuration lives in `.env`; `.env.example` documents supported
variables. Important defaults:

- `TRINO_CATALOG=iceberg`
- `GLUE_DATABASE_RAW=nyc_lakehouse`
- `GLUE_DATABASE_STAGING=staging`
- `GLUE_DATABASE_GOLD=gold`

The dbt profile is repo-owned at `dbt/profiles/profiles.yml` and is mounted into
the Airflow containers at `/home/airflow/.dbt`.

## Data layout

Local raw files under `data/raw/yellow_tripdata_*.parquet` are uploaded to:

```text
s3://$S3_BUCKET/$RAW_PREFIX/yearYYYY/yellow_tripdata_YYYY-MM.parquet
```

The taxi zone reference file is uploaded to:

```text
s3://$S3_BUCKET/$REFERENCE_PREFIX/taxi_zone_lookup.csv
```
