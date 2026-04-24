#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "${PROJECT_ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/.env"
  set +a
fi

export AWS_PROFILE="${AWS_PROFILE:-nyc-lakehouse}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-${AWS_REGION}}"
export AWS_CONFIG_DIR="${AWS_CONFIG_DIR:-${HOME}/.aws}"

export S3_BUCKET="${S3_BUCKET:-nyc-taxi-mpp-lakehouse}"
export RAW_PREFIX="${RAW_PREFIX:-raw/nyc_taxi/yellow_tripdata/}"
export REFERENCE_PREFIX="${REFERENCE_PREFIX:-raw/nyc_taxi/reference/}"
export WAREHOUSE_PREFIX="${WAREHOUSE_PREFIX:-warehouse/}"

export TRINO_CATALOG="${TRINO_CATALOG:-iceberg}"
export GLUE_DATABASE_RAW="${GLUE_DATABASE_RAW:-nyc_lakehouse}"
export GLUE_DATABASE_STAGING="${GLUE_DATABASE_STAGING:-staging}"
export GLUE_DATABASE_GOLD="${GLUE_DATABASE_GOLD:-gold}"
export SPARK_CATALOG_NAME="${SPARK_CATALOG_NAME:-demo}"
export RAW_TRIP_MONTHS="${RAW_TRIP_MONTHS:-2025-01,2025-02}"

export DBT_PROFILE_NAME="${DBT_PROFILE_NAME:-nyc_taxi_dbt}"
export DBT_TARGET="${DBT_TARGET:-dev}"
export TRINO_HOST="${TRINO_HOST:-trino}"
export TRINO_PORT="${TRINO_PORT:-8080}"
export TRINO_USER="${TRINO_USER:-airflow}"
