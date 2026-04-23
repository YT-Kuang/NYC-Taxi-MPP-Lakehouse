from __future__ import annotations

import boto3
import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


AWS_REGION = "us-east-1"
S3_BUCKET = "nyc-taxi-mpp-lakehouse"
RAW_PREFIX = "raw/nyc_taxi/yellow_tripdata/"

# Docker container 內路徑 (非本機路徑)
PROJECT_ROOT = "/opt/nyc_taxi_lakehouse"
SCRIPTS_DIR = f"{PROJECT_ROOT}/scripts"
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbt/nyc_taxi_dbt"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

BRONZE_SCRIPT = f"{SCRIPTS_DIR}/01_bronze_ingest.py"
SILVER_SCRIPT = f"{SCRIPTS_DIR}/02_silver_transform.py"

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

SPARK_SUBMIT_BASE = f"""
export AWS_PROFILE=nyc-lakehouse
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
cd {PROJECT_ROOT}

spark-submit \\
  --master 'local[2]' \\
  --driver-memory 4g \\
  --conf spark.executor.memory=4g \\
  --conf spark.sql.shuffle.partitions=8 \\
  --conf spark.default.parallelism=2 \\
  --packages {SPARK_PACKAGES} \\
"""

DEFAULT_ENV = {
    "AWS_PROFILE": "nyc-lakehouse",
    "AWS_REGION": "us-east-1",
    "AWS_DEFAULT_REGION": "us-east-1",
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "PROJECT_ROOT": PROJECT_ROOT,
    "PYTHONPATH": PROJECT_ROOT,
}


def check_raw_data_exists():
    s3 = boto3.client("s3", region_name=AWS_REGION)
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=RAW_PREFIX,
        MaxKeys=10,
    )

    if "Contents" not in resp or len(resp["Contents"]) == 0:
        raise AirflowException(
            f"No raw files found under s3://{S3_BUCKET}/{RAW_PREFIX}"
        )

    parquet_keys = [obj["Key"] for obj in resp["Contents"] if obj["Key"].endswith(".parquet")]

    if len(parquet_keys) == 0:
        raise AirflowException(
            f"Objects exist under s3://{S3_BUCKET}/{RAW_PREFIX}, but no parquet files were found."
        )

    print(f"Found {len(parquet_keys)} parquet file(s) under s3://{S3_BUCKET}/{RAW_PREFIX}")
    for key in parquet_keys[:10]:
        print(f" - {key}")


with DAG(
    dag_id="nyc_taxi_lakehouse",
    description="NYC Taxi lakehouse pipeline: raw check -> bronze -> silver -> dbt run -> dbt test",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    schedule=None,
    catchup=False, # 不會自動回補以前的排程
    max_active_runs=1,
    default_args={
        "owner": "data-eng",
        "retries": 1,
    },
    tags=["nyc_taxi", "lakehouse", "spark", "dbt", "trino", "iceberg"],
) as dag:

    check_raw = PythonOperator(
        task_id="check_raw_data_exists",
        python_callable=check_raw_data_exists,
    )

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        env=DEFAULT_ENV,
        append_env=True,
        bash_command=f"""
set -euo pipefail
{SPARK_SUBMIT_BASE} {BRONZE_SCRIPT}
""",
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        env=DEFAULT_ENV,
        append_env=True,
        bash_command=f"""
set -euo pipefail
{SPARK_SUBMIT_BASE} {SILVER_SCRIPT}
""",
    )
    # 做 project parsing / model graph 檢查 (之後可省)
    dbt_parse = BashOperator(
        task_id="dbt_parse",
        env=DEFAULT_ENV,
        append_env=True,
        bash_command=f"""
set -euo pipefail
export AWS_PROFILE=nyc-lakehouse
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1

cd {DBT_PROJECT_DIR}
dbt parse --profiles-dir {DBT_PROFILES_DIR}
""",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        env=DEFAULT_ENV,
        append_env=True,
        bash_command=f"""
set -euo pipefail
export AWS_PROFILE=nyc-lakehouse
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1

cd {DBT_PROJECT_DIR}
dbt run --profiles-dir {DBT_PROFILES_DIR}
""",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        env=DEFAULT_ENV,
        append_env=True,
        bash_command=f"""
set -euo pipefail
export AWS_PROFILE=nyc-lakehouse
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1

cd {DBT_PROJECT_DIR}
dbt test --profiles-dir {DBT_PROFILES_DIR}
""",
    )

    check_raw >> bronze_ingest >> silver_transform >> dbt_parse >> dbt_run >> dbt_test