SHELL := /bin/bash

.PHONY: help bootstrap install setup-python validate-aws bootstrap-aws upload-raw up down restart ps logs validate airflow-shell trino-shell \
        test-raw test-bronze test-silver test-dbt-parse test-dbt-run test-dbt-test \
        trigger-dag dbt-debug trino-cli

help:
	@echo "Available commands:"
	@echo "  make bootstrap         - set up Python, AWS resources, raw S3 data, Docker, and validations"
	@echo "  make install           - install local Python dependencies into the current environment"
	@echo "  make setup-python      - create .venv and install local Python dependencies"
	@echo "  make validate-aws      - validate AWS CLI credentials/profile"
	@echo "  make bootstrap-aws     - create/validate S3 bucket and Glue databases"
	@echo "  make upload-raw        - upload local raw data to the expected S3 layout"
	@echo "  make up                - build and start Docker services"
	@echo "  make down              - stop Docker services"
	@echo "  make restart           - restart Docker services"
	@echo "  make ps                - show Docker service status"
	@echo "  make logs              - show Docker compose logs"
	@echo "  make validate          - validate Airflow, dbt, and Trino stack readiness"
	@echo "  make airflow-shell     - open shell in airflow-webserver container"
	@echo "  make trino-shell       - open shell in trino container"
	@echo "  make dbt-debug         - run dbt debug in airflow container"
	@echo "  make test-raw          - test Airflow raw check task"
	@echo "  make test-bronze       - test Airflow bronze task"
	@echo "  make test-silver       - test Airflow silver task"
	@echo "  make test-dbt-parse    - test Airflow dbt_parse task"
	@echo "  make test-dbt-run      - test Airflow dbt_run task"
	@echo "  make test-dbt-test     - test Airflow dbt_test task"
	@echo "  make trigger-dag       - trigger full Airflow DAG"

install:
	python3 -m pip install -r requirements.txt

setup-python:
	./scripts/setup_local_python.sh

validate-aws:
	./scripts/validate_aws.sh

bootstrap-aws:
	./scripts/bootstrap_aws.sh

upload-raw:
	./scripts/upload_raw_data.sh

bootstrap:
	./scripts/bootstrap.sh

up:
	docker compose up -d --build

down:
	docker compose down

restart:
	docker compose restart airflow-webserver airflow-scheduler trino

ps:
	docker compose ps

logs:
	docker compose logs --tail=200

validate:
	./scripts/validate_stack.sh

airflow-shell:
	docker exec -it airflow-webserver bash

trino-shell:
	docker exec -it trino bash

dbt-debug:
	docker exec -it airflow-webserver bash -lc "cd /opt/nyc_taxi_lakehouse/dbt/nyc_taxi_dbt && dbt debug --profiles-dir /home/airflow/.dbt"

test-raw:
	docker exec -it airflow-webserver bash -lc "airflow tasks test nyc_taxi_lakehouse check_raw_data_exists 2026-04-17"

test-bronze:
	docker exec -it airflow-webserver bash -lc "airflow tasks test nyc_taxi_lakehouse bronze_ingest 2026-04-17"

test-silver:
	docker exec -it airflow-webserver bash -lc "airflow tasks test nyc_taxi_lakehouse silver_transform 2026-04-17"

test-dbt-parse:
	docker exec -it airflow-webserver bash -lc "airflow tasks test nyc_taxi_lakehouse dbt_parse 2026-04-17"

test-dbt-run:
	docker exec -it airflow-webserver bash -lc "airflow tasks test nyc_taxi_lakehouse dbt_run 2026-04-17"

test-dbt-test:
	docker exec -it airflow-webserver bash -lc "airflow tasks test nyc_taxi_lakehouse dbt_test 2026-04-17"

trigger-dag:
	docker exec -it airflow-webserver bash -lc "airflow dags trigger nyc_taxi_lakehouse"

trino-cli:
	docker exec -it trino trino
