#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/env.sh"

docker compose ps
docker exec airflow-webserver airflow dags list | grep -q "nyc_taxi_lakehouse"
docker exec airflow-webserver bash -lc "cd /opt/nyc_taxi_lakehouse/dbt/nyc_taxi_dbt && dbt debug --profiles-dir /home/airflow/.dbt"
docker exec trino trino --execute "SHOW CATALOGS" | grep -q "${TRINO_CATALOG}"

echo "Stack validation passed."
