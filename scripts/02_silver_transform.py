import os

from pyspark.sql import SparkSession, functions as F

BUCKET = os.environ.get("S3_BUCKET", "nyc-taxi-mpp-lakehouse")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_PROFILE = os.environ.get("AWS_PROFILE", "nyc-lakehouse")
WAREHOUSE_PREFIX = os.environ.get("WAREHOUSE_PREFIX", "warehouse/").strip("/")
CATALOG = os.environ.get("SPARK_CATALOG_NAME", "demo")
DB = os.environ.get("GLUE_DATABASE_RAW", "nyc_lakehouse")
WAREHOUSE = f"s3://{BUCKET}/{WAREHOUSE_PREFIX}/{DB}/"

spark = (
    SparkSession.builder
    .appName("nyc-taxi-silver-transform")
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

BRONZE_TRIP = f"{CATALOG}.{DB}.bronze_yellow_trip_raw"
BRONZE_ZONE = f"{CATALOG}.{DB}.taxi_zone_lookup_raw"

SILVER_TRIP_CLEAN = f"{CATALOG}.{DB}.silver_yellow_trip_clean"
SILVER_DIM_ZONE = f"{CATALOG}.{DB}.silver_dim_taxi_zone"
SILVER_TRIP_ENRICHED = f"{CATALOG}.{DB}.silver_trip_enriched"

# ----------------------------
# Read bronze tables
# ----------------------------
trip_df = spark.table(BRONZE_TRIP)
zone_raw_df = spark.table(BRONZE_ZONE)

bronze_trip_count = trip_df.count()
bronze_zone_count = zone_raw_df.count()

print("=== BRONZE TABLE COUNTS ===")
print("bronze_yellow_trip_raw:", bronze_trip_count)
print("taxi_zone_lookup_raw:", bronze_zone_count)

# ----------------------------
# Clean taxi zone lookup
# ----------------------------
zone_df = (
    zone_raw_df
    .select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.trim(F.col("Borough")).alias("borough"),
        F.trim(F.col("Zone")).alias("zone"),
        F.trim(F.col("service_zone")).alias("service_zone")
    )
    .filter(F.col("location_id").isNotNull())
    .dropDuplicates(["location_id"])
)

# Optional zone standardization
zone_df = (
    zone_df
    .withColumn(
        "borough",
        F.when(F.col("borough").isin("Unknown", "N/A", ""), "Unknown").otherwise(F.col("borough"))
    )
    .withColumn(
        "service_zone",
        F.when(F.col("service_zone").isin("Unknown", "N/A", ""), "Unknown").otherwise(F.col("service_zone"))
    )
)

silver_zone_count = zone_df.count()

# ----------------------------
# Trip type casting
# ----------------------------
trip_typed_df = (
    trip_df
    .withColumn("VendorID", F.col("VendorID").cast("int"))
    .withColumn("passenger_count", F.col("passenger_count").cast("int"))
    .withColumn("trip_distance", F.col("trip_distance").cast("double"))
    .withColumn("RatecodeID", F.col("RatecodeID").cast("int"))
    .withColumn("payment_type", F.col("payment_type").cast("int"))
    .withColumn("fare_amount", F.col("fare_amount").cast("double"))
    .withColumn("extra", F.col("extra").cast("double"))
    .withColumn("mta_tax", F.col("mta_tax").cast("double"))
    .withColumn("tip_amount", F.col("tip_amount").cast("double"))
    .withColumn("tolls_amount", F.col("tolls_amount").cast("double"))
    .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast("double"))
    .withColumn("total_amount", F.col("total_amount").cast("double"))
    .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast("double"))
    .withColumn("Airport_fee", F.col("Airport_fee").cast("double"))
    .withColumn("PULocationID", F.col("PULocationID").cast("int"))
    .withColumn("DOLocationID", F.col("DOLocationID").cast("int"))
    .withColumn("store_and_fwd_flag", F.upper(F.trim(F.col("store_and_fwd_flag"))))
    .withColumn("tpep_pickup_datetime", F.to_timestamp("tpep_pickup_datetime"))
    .withColumn("tpep_dropoff_datetime", F.to_timestamp("tpep_dropoff_datetime"))
)

# ----------------------------
# Deduplicate trips
# ----------------------------
trip_dedup_df = trip_typed_df.dropDuplicates([
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "total_amount"
])

dedup_trip_count = trip_dedup_df.count()
duplicate_removed_count = bronze_trip_count - dedup_trip_count

# ----------------------------
# Derived fields before filtering
# ----------------------------
trip_enriched_base_df = (
    trip_dedup_df
    .withColumn(
        "trip_duration_min",
        (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")) / 60.0
    )
    .withColumn("trip_hours", F.col("trip_duration_min") / 60.0)
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
    .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
    .withColumn("pickup_day", F.dayofmonth("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("pickup_weekday", F.date_format("tpep_pickup_datetime", "E"))
    .withColumn(
        "trip_speed_mph",
        F.when(F.col("trip_hours") > 0, F.col("trip_distance") / F.col("trip_hours"))
    )
    .withColumn(
        "fare_per_mile",
        F.when(F.col("trip_distance") > 0, F.col("fare_amount") / F.col("trip_distance"))
    )
)

# ----------------------------
# Standardize coded/status fields
# ----------------------------
trip_enriched_base_df = (
    trip_enriched_base_df
    .withColumn(
        "payment_type_desc",
        F.when(F.col("payment_type") == 1, "Credit card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .when(F.col("payment_type") == 5, "Unknown")
         .when(F.col("payment_type") == 6, "Voided trip")
         .otherwise("Other")
    )
    .withColumn(
        "rate_code_desc",
        F.when(F.col("RatecodeID") == 1, "Standard rate")
         .when(F.col("RatecodeID") == 2, "JFK")
         .when(F.col("RatecodeID") == 3, "Newark")
         .when(F.col("RatecodeID") == 4, "Nassau/Westchester")
         .when(F.col("RatecodeID") == 5, "Negotiated fare")
         .when(F.col("RatecodeID") == 6, "Group ride")
         .otherwise("Unknown")
    )
    .withColumn(
        "store_and_fwd_flag_std",
        F.when(F.col("store_and_fwd_flag") == "Y", "Y")
         .when(F.col("store_and_fwd_flag") == "N", "N")
         .otherwise("Unknown")
    )
)

# Airport trip flag
trip_enriched_base_df = trip_enriched_base_df.withColumn(
    "is_airport_trip",
    F.when(
        (F.col("RatecodeID").isin(2, 3)) | (F.coalesce(F.col("Airport_fee"), F.lit(0.0)) > 0),
        F.lit(True)
    ).otherwise(F.lit(False))
)

# ----------------------------
# Build DQ flags
# ----------------------------
trip_flagged_df = (
    trip_enriched_base_df
    .withColumn("dq_missing_pickup", F.col("tpep_pickup_datetime").isNull())
    .withColumn("dq_missing_dropoff", F.col("tpep_dropoff_datetime").isNull())
    .withColumn("dq_missing_pu_location", F.col("PULocationID").isNull())
    .withColumn("dq_missing_do_location", F.col("DOLocationID").isNull())
    .withColumn("dq_missing_trip_distance", F.col("trip_distance").isNull())
    .withColumn("dq_missing_fare_amount", F.col("fare_amount").isNull())
    .withColumn("dq_missing_total_amount", F.col("total_amount").isNull())
    .withColumn("dq_missing_passenger_count", F.col("passenger_count").isNull())
    .withColumn("dq_invalid_trip_distance", F.col("trip_distance") <= 0)
    .withColumn("dq_invalid_fare_amount", F.col("fare_amount") < 0)
    .withColumn("dq_invalid_total_amount", F.col("total_amount") < 0)
    .withColumn("dq_invalid_passenger_count", F.col("passenger_count") <= 0)
    .withColumn("dq_invalid_duration", (F.col("trip_duration_min") <= 0) | (F.col("trip_duration_min") > 300))
    .withColumn("dq_invalid_distance_outlier", F.col("trip_distance") > 100)
    .withColumn("dq_invalid_fare_outlier", F.col("fare_amount") > 500)
    .withColumn("dq_invalid_total_outlier", F.col("total_amount") > 600)
    .withColumn(
        "dq_invalid_speed",
        F.col("trip_speed_mph").isNotNull() & ((F.col("trip_speed_mph") < 1) | (F.col("trip_speed_mph") > 80))
    )
)

dq_issue_expr = (
    F.col("dq_missing_pickup") |
    F.col("dq_missing_dropoff") |
    F.col("dq_missing_pu_location") |
    F.col("dq_missing_do_location") |
    F.col("dq_missing_trip_distance") |
    F.col("dq_missing_fare_amount") |
    F.col("dq_missing_total_amount") |
    F.col("dq_missing_passenger_count") |
    F.col("dq_invalid_trip_distance") |
    F.col("dq_invalid_fare_amount") |
    F.col("dq_invalid_total_amount") |
    F.col("dq_invalid_passenger_count") |
    F.col("dq_invalid_duration") |
    F.col("dq_invalid_distance_outlier") |
    F.col("dq_invalid_fare_outlier") |
    F.col("dq_invalid_total_outlier") |
    F.col("dq_invalid_speed")
)

trip_flagged_df = trip_flagged_df.withColumn(
    "is_valid_trip",
    ~dq_issue_expr
)

# DQ summary
dq_summary = trip_flagged_df.agg(
    F.count("*").alias("total_after_dedup"),
    F.sum(F.when(F.col("dq_missing_pickup"), 1).otherwise(0)).alias("missing_pickup_cnt"),
    F.sum(F.when(F.col("dq_missing_dropoff"), 1).otherwise(0)).alias("missing_dropoff_cnt"),
    F.sum(F.when(F.col("dq_missing_pu_location"), 1).otherwise(0)).alias("missing_pu_location_cnt"),
    F.sum(F.when(F.col("dq_missing_do_location"), 1).otherwise(0)).alias("missing_do_location_cnt"),
    F.sum(F.when(F.col("dq_invalid_trip_distance"), 1).otherwise(0)).alias("invalid_trip_distance_cnt"),
    F.sum(F.when(F.col("dq_invalid_duration"), 1).otherwise(0)).alias("invalid_duration_cnt"),
    F.sum(F.when(F.col("dq_invalid_speed"), 1).otherwise(0)).alias("invalid_speed_cnt"),
    F.sum(F.when(~F.col("is_valid_trip"), 1).otherwise(0)).alias("total_invalid_cnt"),
    F.sum(F.when(F.col("is_valid_trip"), 1).otherwise(0)).alias("total_valid_cnt")
).collect()[0]

print("=== DQ SUMMARY ===")
print("bronze_trip_count =", bronze_trip_count)
print("dedup_trip_count =", dedup_trip_count)
print("duplicate_removed_count =", duplicate_removed_count)
print("missing_pickup_cnt =", dq_summary["missing_pickup_cnt"])
print("missing_dropoff_cnt =", dq_summary["missing_dropoff_cnt"])
print("missing_pu_location_cnt =", dq_summary["missing_pu_location_cnt"])
print("missing_do_location_cnt =", dq_summary["missing_do_location_cnt"])
print("invalid_trip_distance_cnt =", dq_summary["invalid_trip_distance_cnt"])
print("invalid_duration_cnt =", dq_summary["invalid_duration_cnt"])
print("invalid_speed_cnt =", dq_summary["invalid_speed_cnt"])
print("total_invalid_cnt =", dq_summary["total_invalid_cnt"])
print("total_valid_cnt =", dq_summary["total_valid_cnt"])

# Optional fail-fast rule
if dq_summary["total_valid_cnt"] == 0:
    raise ValueError("DQ check failed: no valid trips remain after cleansing.")

# Keep only valid trips for silver clean table
trip_clean_df = trip_flagged_df.filter(F.col("is_valid_trip") == True)

silver_trip_clean_count = trip_clean_df.count()

# ----------------------------
# Enrich with pickup/dropoff zones
# ----------------------------
pu_zone_df = zone_df.select(
    F.col("location_id").alias("pu_location_id"),
    F.col("borough").alias("pu_borough"),
    F.col("zone").alias("pu_zone"),
    F.col("service_zone").alias("pu_service_zone")
)

do_zone_df = zone_df.select(
    F.col("location_id").alias("do_location_id"),
    F.col("borough").alias("do_borough"),
    F.col("zone").alias("do_zone"),
    F.col("service_zone").alias("do_service_zone")
)

trip_enriched_df = (
    trip_clean_df
    .join(pu_zone_df, trip_clean_df["PULocationID"] == pu_zone_df["pu_location_id"], "left")
    .join(do_zone_df, trip_clean_df["DOLocationID"] == do_zone_df["do_location_id"], "left")
)

silver_trip_enriched_count = trip_enriched_df.count()

# ----------------------------
# Write silver tables
# ----------------------------
zone_df.writeTo(SILVER_DIM_ZONE) \
    .tableProperty("format-version", "2") \
    .using("iceberg") \
    .createOrReplace()

trip_clean_df.writeTo(SILVER_TRIP_CLEAN) \
    .tableProperty("format-version", "2") \
    .using("iceberg") \
    .createOrReplace()

trip_enriched_df.writeTo(SILVER_TRIP_ENRICHED) \
    .tableProperty("format-version", "2") \
    .using("iceberg") \
    .createOrReplace()

print("=== SILVER TABLES ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.{DB}").show(truncate=False)

print("silver_dim_taxi_zone:", silver_zone_count)
print("silver_yellow_trip_clean:", silver_trip_clean_count)
print("silver_trip_enriched:", silver_trip_enriched_count)

print("Silver tables created successfully.")

spark.stop()
