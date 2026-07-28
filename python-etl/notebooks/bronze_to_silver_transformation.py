# Databricks notebook source
# Cell 1 — Configure ADLS Gen2 connection
# Key is read from cluster environment variable — never hardcoded

import os

storage_account = "nyctaxistorageps"
container = "medallion"

# Read key from environment variable (set in cluster config)
storage_key = os.environ.get("AZURE_STORAGE_KEY")

if not storage_key:
    raise ValueError("AZURE_STORAGE_KEY environment variable not set in cluster config")

# Authenticate Spark to access ADLS Gen2
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Define paths — used throughout all cells
BASE   = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
BRONZE = f"{BASE}/bronze/raw/yellow_taxi/year=2024/month=01/"
SILVER = f"{BASE}/silver/cleaned/yellow_taxi/"

print("✅ ADLS connection configured!")
print(f"Bronze path: {BRONZE}")
print(f"Silver path: {SILVER}")

# COMMAND ----------

# Cell 2 — Read NYC taxi data from bronze layer
# This reads the real 47.65MB Parquet file you ingested on Day 3

df_bronze = spark.read.parquet(BRONZE)

# Basic exploration
print(f"✅ Data loaded successfully!")
print(f"Total rows: {df_bronze.count():,}")
print(f"Total columns: {len(df_bronze.columns)}")
print(f"\nSchema:")
df_bronze.printSchema()
print(f"\nSample data (5 rows):")
df_bronze.show(5, truncate=True)

# COMMAND ----------

# Cell 3 — Data quality analysis
# Understand what needs to be cleaned before writing to silver

from pyspark.sql.functions import col, count, when, isnan, isnull, min, max, avg

print("=" * 60)
print("DATA QUALITY REPORT — Bronze Layer")
print("=" * 60)

# 1. Check for NULLs in key columns
print("\n📊 NULL counts in key columns:")
df_bronze.select([
    count(when(isnull(c), c)).alias(c)
    for c in ["VendorID", "passenger_count", "trip_distance",
              "fare_amount", "total_amount", "PULocationID", "DOLocationID"]
]).show()

# 2. Check for invalid values
print("📊 Invalid value counts:")
df_bronze.select(
    count(when(col("fare_amount") <= 0, True)).alias("negative_fare"),
    count(when(col("trip_distance") <= 0, True)).alias("zero_distance"),
    count(when(col("passenger_count") <= 0, True)).alias("zero_passengers"),
    count(when(col("passenger_count") > 6, True)).alias("too_many_passengers"),
    count(when(col("tpep_pickup_datetime") > col("tpep_dropoff_datetime"), True)).alias("invalid_timestamps"),
).show()

# 3. Basic stats on key numeric columns
print("📊 Basic statistics:")
df_bronze.select(
    "fare_amount", "trip_distance",
    "passenger_count", "total_amount"
).describe().show()

# COMMAND ----------

# Cell 4 — Clean data and write to silver layer
# Apply data quality rules based on our analysis

from pyspark.sql.functions import (
    col, year, month, to_date,
    unix_timestamp, round
)

print("Starting bronze → silver transformation...")
print(f"Input rows: {df_bronze.count():,}")

# ── Step 1: Rename columns to snake_case ──────────────────
df_renamed = df_bronze \
    .withColumnRenamed("VendorID", "vendor_id") \
    .withColumnRenamed("RatecodeID", "rate_code_id") \
    .withColumnRenamed("PULocationID", "pickup_location_id") \
    .withColumnRenamed("DOLocationID", "dropoff_location_id") \
    .withColumnRenamed("Airport_fee", "airport_fee") \
    .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

# ── Step 2: Fix data types ─────────────────────────────────
df_typed = df_renamed \
    .withColumn("vendor_id",          col("vendor_id").cast("integer")) \
    .withColumn("rate_code_id",       col("rate_code_id").cast("integer")) \
    .withColumn("payment_type",       col("payment_type").cast("integer")) \
    .withColumn("passenger_count",    col("passenger_count").cast("integer")) \
    .withColumn("fare_amount",        round(col("fare_amount"), 2)) \
    .withColumn("total_amount",       round(col("total_amount"), 2)) \
    .withColumn("tip_amount",         round(col("tip_amount"), 2)) \
    .withColumn("trip_distance",      round(col("trip_distance"), 2))

# ── Step 3: Add derived columns ────────────────────────────
df_enriched = df_typed \
    .withColumn("pickup_date",  to_date(col("pickup_datetime"))) \
    .withColumn("pickup_year",  year(col("pickup_datetime"))) \
    .withColumn("pickup_month", month(col("pickup_datetime"))) \
    .withColumn("trip_duration_mins",
        round((unix_timestamp("dropoff_datetime") -
               unix_timestamp("pickup_datetime")) / 60, 2))

# ── Step 4: Apply data quality filters ────────────────────
df_silver = df_enriched \
    .filter(col("fare_amount") > 0) \
    .filter(col("trip_distance") > 0) \
    .filter(col("trip_distance") < 500) \
    .filter(col("passenger_count") > 0) \
    .filter(col("passenger_count") <= 6) \
    .filter(col("total_amount") > 0) \
    .filter(col("pickup_datetime") < col("dropoff_datetime")) \
    .filter(col("passenger_count").isNotNull()) \
    .filter(col("trip_duration_mins") > 0) \
    .filter(col("trip_duration_mins") < 300)\
    .filter(col("pickup_year") == 2024) \
    .filter(col("pickup_month") == 1)
# ── Step 5: Summary ───────────────────────────────────────
silver_count = df_silver.count()
bronze_count = 2964624
removed = bronze_count - silver_count

print(f"\n📊 Transformation Summary:")
print(f"Bronze rows:  {bronze_count:,}")
print(f"Silver rows:  {silver_count:,}")
print(f"Rows removed: {removed:,} ({removed/bronze_count*100:.1f}%)")
print(f"\nSample silver data:")
df_silver.select(
    "vendor_id", "pickup_datetime", "pickup_location_id",
    "dropoff_location_id", "passenger_count",
    "trip_distance", "fare_amount", "total_amount",
    "trip_duration_mins"
).show(5)

# COMMAND ----------

# Cell 5 — Write cleaned data to ADLS silver layer as Delta table

print("Writing to silver layer...")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month") \
    .save(SILVER)

print("✅ Silver layer written successfully!")
print(f"Location: {SILVER}")

# Verify by reading back
df_verify = spark.read.format("delta").load(SILVER)
print(f"Verification — rows in silver: {df_verify.count():,}")
print(f"Partitions created:")
spark.sql(f"SELECT pickup_year, pickup_month, COUNT(*) as rows FROM delta.`{SILVER}` GROUP BY 1,2").show()

# COMMAND ----------

# Cell 6 — Analyze silver layer data
# Prove the data is clean and useful

print("📊 Silver Layer Analysis — January 2024 NYC Taxi Trips")
print("=" * 55)

# Revenue by vendor
print("\n💰 Revenue by vendor:")
spark.sql(f"""
    SELECT
        vendor_id,
        COUNT(*)                    AS trip_count,
        ROUND(SUM(fare_amount), 2)  AS total_fare,
        ROUND(AVG(fare_amount), 2)  AS avg_fare,
        ROUND(AVG(trip_distance), 2) AS avg_distance
    FROM delta.`{SILVER}`
    GROUP BY vendor_id
    ORDER BY total_fare DESC
""").show()

# Peak hours
print("\n🕐 Top 5 busiest hours:")
spark.sql(f"""
    SELECT
        HOUR(pickup_datetime)  AS hour,
        COUNT(*)               AS trip_count,
        ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM delta.`{SILVER}`
    GROUP BY HOUR(pickup_datetime)
    ORDER BY trip_count DESC
    LIMIT 5
""").show()

# Trip distance distribution
print("\n🚕 Trip size breakdown:")
spark.sql(f"""
    SELECT
        CASE
            WHEN trip_distance <= 1  THEN 'Short (0-1 mile)'
            WHEN trip_distance <= 3  THEN 'Medium (1-3 miles)'
            WHEN trip_distance <= 10 THEN 'Long (3-10 miles)'
            ELSE 'Very Long (10+ miles)'
        END AS trip_size,
        COUNT(*) AS trips,
        ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM delta.`{SILVER}`
    GROUP BY 1
    ORDER BY trips DESC
""").show()

# COMMAND ----------

# Cell 7 — Read silver layer + create dimension DataFrames
# Starting point for gold layer / fact table creation

from pyspark.sql.functions import broadcast, col, year, month, hour

# ── Read silver layer (already cleaned + written on Day 8) ──
df_silver = spark.read.format("delta").load(SILVER)
print(f"✅ Silver data loaded: {df_silver.count():,} rows")

# ── Create dimension DataFrames ──────────────────────────────
# These are small lookup tables — perfect for broadcast joins

# dim_vendor (2 rows)
dim_vendor = spark.createDataFrame([
    (1, "Creative Mobile Technologies"),
    (2, "VeriFone Inc"),
], ["vendor_id", "vendor_name"])

# dim_payment_type (6 rows)
dim_payment = spark.createDataFrame([
    (1, "Credit Card"),
    (2, "Cash"),
    (3, "No Charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided Trip"),
], ["payment_type", "payment_name"])

# dim_rate (6 rows)
dim_rate = spark.createDataFrame([
    (1, "Standard Rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau/Westchester"),
    (5, "Negotiated Fare"),
    (6, "Group Ride"),
], ["rate_code_id", "rate_name"])

print("✅ Dimension DataFrames created!")
print(f"   dim_vendor:  {dim_vendor.count()} rows")
print(f"   dim_payment: {dim_payment.count()} rows")
print(f"   dim_rate:    {dim_rate.count()} rows")

# COMMAND ----------

# Cell 8 — Build fact_taxi_trips (Gold Layer)
# Join silver data with dimension tables

from pyspark.sql.functions import broadcast, col, monotonically_increasing_id

print("Building fact_taxi_trips...")

# ── Join silver with dimensions using broadcast ──────────────
fact_taxi_trips = df_silver \
    .join(broadcast(dim_vendor), on="vendor_id", how="left") \
    .join(broadcast(dim_payment), on="payment_type", how="left") \
    .join(broadcast(dim_rate), on="rate_code_id", how="left") \
    .select(
        # Surrogate key
        monotonically_increasing_id().alias("trip_surrogate_key"),

        # Foreign keys to dimensions
        col("vendor_id"),
        col("vendor_name"),
        col("pickup_location_id"),
        col("dropoff_location_id"),
        col("rate_code_id"),
        col("rate_name"),
        col("payment_type"),
        col("payment_name"),

        # Date/time
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_date"),
        col("pickup_year"),
        col("pickup_month"),
        col("trip_duration_mins"),

        # Measures (facts)
        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"),
        col("tolls_amount"),
        col("total_amount"),
        col("congestion_surcharge"),
        col("airport_fee"),
    )

# ── Summary ───────────────────────────────────────────────────
fact_count = fact_taxi_trips.count()
print(f"✅ fact_taxi_trips built!")
print(f"   Rows: {fact_count:,}")
print(f"   Columns: {len(fact_taxi_trips.columns)}")
print(f"\nSample:")
fact_taxi_trips.select(
    "vendor_name", "payment_name", "rate_name",
    "trip_distance", "fare_amount", "total_amount",
    "trip_duration_mins"
).show(5, truncate=True)

# COMMAND ----------

# Cell 9 — Write fact_taxi_trips to gold layer

GOLD = f"{BASE}/gold/star_schema/fact_taxi_trips/"

print("Writing fact_taxi_trips to gold layer...")

fact_taxi_trips.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month") \
    .save(GOLD)

print("✅ Gold layer written successfully!")
print(f"Location: {GOLD}")

# Verify
df_gold = spark.read.format("delta").load(GOLD)
print(f"Verification — rows in gold: {df_gold.count():,}")
print(f"\nPartitions:")
spark.sql(f"""
    SELECT pickup_year, pickup_month, COUNT(*) as rows
    FROM delta.`{GOLD}`
    GROUP BY 1, 2
""").show()

# COMMAND ----------

# Cell 10 — Business analysis on gold layer
# This is what Power BI will query

print("📊 GOLD LAYER ANALYSIS — NYC Taxi January 2024")
print("=" * 55)

# Revenue by vendor
print("\n💰 Revenue by vendor:")
spark.sql(f"""
    SELECT vendor_name,
           COUNT(*)                     AS total_trips,
           ROUND(SUM(fare_amount), 2)   AS total_revenue,
           ROUND(AVG(fare_amount), 2)   AS avg_fare,
           ROUND(AVG(trip_distance), 2) AS avg_distance,
           ROUND(AVG(trip_duration_mins), 2) AS avg_duration_mins
    FROM delta.`{GOLD}`
    GROUP BY vendor_name
    ORDER BY total_revenue DESC
""").show()

# Payment method breakdown
print("\n💳 Payment method breakdown:")
spark.sql(f"""
    SELECT payment_name,
           COUNT(*) AS trips,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
    FROM delta.`{GOLD}`
    GROUP BY payment_name
    ORDER BY trips DESC
""").show()

# Hourly trip pattern
print("\n🕐 Revenue by hour of day:")
spark.sql(f"""
    SELECT HOUR(pickup_datetime) AS hour,
           COUNT(*) AS trips,
           ROUND(SUM(fare_amount), 2) AS revenue,
           ROUND(AVG(fare_amount), 2) AS avg_fare
    FROM delta.`{GOLD}`
    GROUP BY 1
    ORDER BY revenue DESC
    LIMIT 10
""").show()

# Top pickup locations
print("\n📍 Top 10 pickup locations:")
spark.sql(f"""
    SELECT pickup_location_id,
           COUNT(*) AS trips,
           ROUND(SUM(fare_amount), 2) AS revenue
    FROM delta.`{GOLD}`
    GROUP BY pickup_location_id
    ORDER BY trips DESC
    LIMIT 10
""").show()

# COMMAND ----------

