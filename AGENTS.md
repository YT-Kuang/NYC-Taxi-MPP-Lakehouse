# AGENTS.md

## Project overview
This repository is an end-to-end local lakehouse analytics project for NYC yellow taxi data.

Main components:
- AWS S3 for raw and warehouse storage
- AWS Glue Catalog for Iceberg metadata
- Trino for SQL querying
- PySpark for bronze/silver processing
- dbt for staging/gold modeling
- Airflow for orchestration
- BI layer for analytics dashboards

## Repository goals
The repository should be reproducible on a fresh machine by:
1. cloning the repo
2. configuring AWS credentials locally
3. running setup/bootstrap scripts
4. starting Docker services
5. running the pipeline end-to-end
6. validating bronze, silver, staging, and gold outputs

## Rules
- Never commit secrets.
- Never commit local machine specific paths.
- Never assume local AWS credentials are stored in git.
- Prefer environment variables and .env.example over hard-coded values.
- Prefer idempotent setup scripts.
- Keep Docker Compose, Airflow, dbt, and Spark setup reproducible.
- Keep README aligned with actual repo behavior.
- Do not claim measured performance improvements unless benchmarked.

## Important paths
- scripts/ : Spark jobs and local setup scripts
- dbt/nyc_taxi_dbt/ : dbt project
- infra/airflow/ : Airflow Dockerfile, dags, requirements
- infra/trino/ : Trino catalog config
- docker-compose.yml : local stack definition

## Validation commands
- docker compose ps
- docker exec -it airflow-webserver airflow dags list
- docker exec -it airflow-webserver dbt debug --profiles-dir /home/airflow/.dbt
- docker exec -it trino trino
- Spark submit smoke tests
- Airflow task tests

## Expectations for changes
When modifying the repository:
- prefer reviewable, minimal changes
- explain new scripts and environment variables
- avoid introducing unnecessary tools
- keep setup friendly for a new machine