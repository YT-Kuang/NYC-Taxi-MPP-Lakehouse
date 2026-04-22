from pyspark.sql import SparkSession

BUCKET = "nyc-taxi-mpp-lakehouse"
WAREHOUSE = f"s3://{BUCKET}/warehouse/nyc_lakehouse/"

spark = (
    SparkSession.builder
    .appName("nyc-taxi-bronze-ingest")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.demo.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.demo.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.defaultCatalog", "demo")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    .config("spark.hadoop.fs.s3a.aws.profile", "nyc-lakehouse")
    .getOrCreate()
)

# 確認 catalog 配好了沒
print("=== CATALOGS ===")
spark.sql("SHOW CATALOGS").show(truncate=False)

# 建 namespace（對 Glue 來說就是 database）
spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.nyc_lakehouse")

# 確認 Glue database / namespace 看得到
print("=== NAMESPACES IN demo ===")
spark.sql("SHOW NAMESPACES IN demo").show(truncate=False)

# 先只吃 1~2 月，確認流程跑通
yellow_df = spark.read.parquet(
    f"s3a://{BUCKET}/raw/nyc_taxi/yellow_tripdata/year2025/yellow_tripdata_2025-01.parquet",
    f"s3a://{BUCKET}/raw/nyc_taxi/yellow_tripdata/year2025/yellow_tripdata_2025-02.parquet"
)

zone_df = spark.read.option("header", True).csv(
    f"s3a://{BUCKET}/raw/nyc_taxi/reference/taxi_zone_lookup.csv"
)

# 降低本機記憶體壓力
yellow_df = yellow_df.coalesce(2)

yellow_df.writeTo("demo.nyc_lakehouse.bronze_yellow_trip_raw") \
    .tableProperty("format-version", "2") \
    .using("iceberg") \
    .createOrReplace()

zone_df.writeTo("demo.nyc_lakehouse.taxi_zone_lookup_raw") \
    .tableProperty("format-version", "2") \
    .using("iceberg") \
    .createOrReplace()

# 確認表真的寫成功了沒
print("=== TABLES IN demo.nyc_lakehouse ===")
spark.sql("SHOW TABLES IN demo.nyc_lakehouse").show(truncate=False)

print("Bronze tables created successfully.")

spark.stop()