from __future__ import annotations

import os

import boto3
import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


AWS_PROFILE = os.environ.get("AWS_PROFILE", "nyc-lakehouse")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_DEFAULT_REGION = os.environ.get("AWS_DEFAULT_REGION", AWS_REGION)
S3_BUCKET = os.environ.get("S3_BUCKET", "nyc-taxi-mpp-lakehouse")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/nyc_taxi/yellow_tripdata/")

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


BRONZE_BASH_COMMAND = f"""
set -euo pipefail

export AWS_PROFILE={AWS_PROFILE}
export AWS_REGION={AWS_REGION}
export AWS_DEFAULT_REGION={AWS_DEFAULT_REGION}

cd {PROJECT_ROOT}
pwd
which spark-submit
ls -l {BRONZE_SCRIPT}

spark-submit \\
  --master 'local[2]' \\
  --driver-memory 4g \\
  --conf spark.executor.memory=4g \\
  --conf spark.sql.shuffle.partitions=8 \\
  --conf spark.default.parallelism=2 \\
  --packages {SPARK_PACKAGES} \\
  {BRONZE_SCRIPT}
"""


SILVER_BASH_COMMAND = f"""
set -euo pipefail

export AWS_PROFILE={AWS_PROFILE}
export AWS_REGION={AWS_REGION}
export AWS_DEFAULT_REGION={AWS_DEFAULT_REGION}

cd {PROJECT_ROOT}
pwd
which spark-submit
ls -l {SILVER_SCRIPT}

spark-submit \\
  --master 'local[1]' \\
  --driver-memory 2g \\
  --conf spark.executor.memory=2g \\
  --conf spark.sql.shuffle.partitions=4 \\
  --conf spark.default.parallelism=1 \\
  --packages {SPARK_PACKAGES} \\
  {SILVER_SCRIPT}
"""

DEFAULT_ENV = {
    "AWS_PROFILE": AWS_PROFILE,
    "AWS_REGION": AWS_REGION,
    "AWS_DEFAULT_REGION": AWS_DEFAULT_REGION,
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "PROJECT_ROOT": PROJECT_ROOT,
    "S3_BUCKET": S3_BUCKET,
    "RAW_PREFIX": RAW_PREFIX,
    "REFERENCE_PREFIX": os.environ.get("REFERENCE_PREFIX", "raw/nyc_taxi/reference/"),
    "WAREHOUSE_PREFIX": os.environ.get("WAREHOUSE_PREFIX", "warehouse/"),
    "TRINO_CATALOG": os.environ.get("TRINO_CATALOG", "iceberg"),
    "GLUE_DATABASE_RAW": os.environ.get("GLUE_DATABASE_RAW", "nyc_lakehouse"),
    "GLUE_DATABASE_STAGING": os.environ.get("GLUE_DATABASE_STAGING", "staging"),
    "GLUE_DATABASE_GOLD": os.environ.get("GLUE_DATABASE_GOLD", "gold"),
    "SPARK_CATALOG_NAME": os.environ.get("SPARK_CATALOG_NAME", "demo"),
    "RAW_TRIP_MONTHS": os.environ.get("RAW_TRIP_MONTHS", "2025-01,2025-02"),
    "DBT_TARGET": os.environ.get("DBT_TARGET", "dev"),
    "TRINO_HOST": os.environ.get("TRINO_HOST", "trino"),
    "TRINO_PORT": os.environ.get("TRINO_PORT", "8080"),
    "TRINO_USER": os.environ.get("TRINO_USER", "airflow"),
}


def make_dbt_bash_command(dbt_command: str) -> str:
    return f"""
set -euo pipefail

export AWS_PROFILE={AWS_PROFILE}
export AWS_REGION={AWS_REGION}
export AWS_DEFAULT_REGION={AWS_DEFAULT_REGION}

cd {DBT_PROJECT_DIR}
dbt {dbt_command} --profiles-dir {DBT_PROFILES_DIR}
"""


def check_raw_data_exists():
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    s3 = session.client("s3")
    resp = s3.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=RAW_PREFIX,
        MaxKeys=10,
    )

    if "Contents" not in resp or len(resp["Contents"]) == 0:
        raise AirflowException(
            f"No raw files found under s3://{S3_BUCKET}/{RAW_PREFIX}"
        )

    parquet_keys = [
        obj["Key"] for obj in resp["Contents"] if obj["Key"].endswith(".parquet")
    ]

    if len(parquet_keys) == 0:
        raise AirflowException(
            f"Objects exist under s3://{S3_BUCKET}/{RAW_PREFIX}, but no parquet files were found."
        )

    print(
        f"Found {len(parquet_keys)} parquet file(s) under s3://{S3_BUCKET}/{RAW_PREFIX}"
    )
    for key in parquet_keys[:10]:
        print(f" - {key}")


with DAG(
    dag_id="nyc_taxi_lakehouse",
    description="NYC Taxi lakehouse pipeline: raw check -> bronze -> silver -> dbt run -> dbt test",
    start_date=pendulum.datetime(2026, 4, 1, tz="UTC"),
    schedule=None,
    catchup=False,  # 不會自動回補以前的排程
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
        bash_command=BRONZE_BASH_COMMAND,
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        env=DEFAULT_ENV,
        append_env=True,
        bash_command=SILVER_BASH_COMMAND,
    )

    # 做 project parsing / model graph 檢查 (之後可省)
    dbt_parse = BashOperator(
        task_id="dbt_parse",
        env=DEFAULT_ENV,
        append_env=True,
        bash_command=make_dbt_bash_command("parse"),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        env=DEFAULT_ENV,
        append_env=True,
        bash_command=make_dbt_bash_command("run"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        env=DEFAULT_ENV,
        append_env=True,
        bash_command=make_dbt_bash_command("test"),
    )

    check_raw >> bronze_ingest >> silver_transform >> dbt_parse >> dbt_run >> dbt_test
