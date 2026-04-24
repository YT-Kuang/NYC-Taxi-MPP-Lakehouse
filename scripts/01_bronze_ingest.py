import os

from pyspark.sql import SparkSession

BUCKET = os.environ.get("S3_BUCKET", "nyc-taxi-mpp-lakehouse")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_PROFILE = os.environ.get("AWS_PROFILE", "nyc-lakehouse")
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/nyc_taxi/yellow_tripdata/").strip("/")
REFERENCE_PREFIX = os.environ.get("REFERENCE_PREFIX", "raw/nyc_taxi/reference/").strip("/")
WAREHOUSE_PREFIX = os.environ.get("WAREHOUSE_PREFIX", "warehouse/").strip("/")
CATALOG = os.environ.get("SPARK_CATALOG_NAME", "demo")
DB = os.environ.get("GLUE_DATABASE_RAW", "nyc_lakehouse")
RAW_TRIP_MONTHS = [
    month.strip()
    for month in os.environ.get("RAW_TRIP_MONTHS", "2025-01,2025-02").split(",")
    if month.strip()
]
WAREHOUSE = f"s3://{BUCKET}/{WAREHOUSE_PREFIX}/{DB}/"

spark = (
    SparkSession.builder
    .appName("nyc-taxi-bronze-ingest")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.defaultCatalog", CATALOG)
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.aws.profile", AWS_PROFILE)
    .getOrCreate()
)

# 確認 catalog 配好了沒
print("=== CATALOGS ===")
spark.sql("SHOW CATALOGS").show(truncate=False)

# 建 namespace（對 Glue 來說就是 database）
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{DB}")

# 確認 Glue database / namespace 看得到
print(f"=== NAMESPACES IN {CATALOG} ===")
spark.sql(f"SHOW NAMESPACES IN {CATALOG}").show(truncate=False)

# 先只吃 1~2 月，確認流程跑通
raw_paths = [
    f"s3a://{BUCKET}/{RAW_PREFIX}/year{month[:4]}/yellow_tripdata_{month}.parquet"
    for month in RAW_TRIP_MONTHS
]
yellow_df = spark.read.parquet(*raw_paths)

zone_df = spark.read.option("header", True).csv(
    f"s3a://{BUCKET}/{REFERENCE_PREFIX}/taxi_zone_lookup.csv"
)

# 降低本機記憶體壓力
yellow_df = yellow_df.coalesce(2)

yellow_df.writeTo(f"{CATALOG}.{DB}.bronze_yellow_trip_raw") \
    .tableProperty("format-version", "2") \
    .using("iceberg") \
    .createOrReplace()

zone_df.writeTo(f"{CATALOG}.{DB}.taxi_zone_lookup_raw") \
    .tableProperty("format-version", "2") \
    .using("iceberg") \
    .createOrReplace()

# 確認表真的寫成功了沒
print(f"=== TABLES IN {CATALOG}.{DB} ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.{DB}").show(truncate=False)

print("Bronze tables created successfully.")

spark.stop()
