# Databricks notebook source
# Data Quality Checks — Gold Layer
# Runs after bronze_to_silver transformation completes
# Validates data quality at each layer

import os
from pyspark.sql.functions import col, count, when, isnull

storage_account = "nyctaxistorageps"
container = "medallion"
storage_key = os.environ.get("AZURE_STORAGE_KEY")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

BASE   = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
SILVER = f"{BASE}/silver/cleaned/yellow_taxi/"
GOLD   = f"{BASE}/gold/star_schema/fact_taxi_trips_enriched/"

print("=" * 55)
print("DATA QUALITY CHECKS — NYC Taxi Pipeline")
print("=" * 55)

# ── Check 1: Silver row count ──────────────────────────────
df_silver = spark.read.format("delta").load(SILVER)
silver_count = df_silver.count()
assert silver_count > 2000000, f"❌ Silver row count too low: {silver_count}"
print(f"✅ Check 1 PASSED — Silver rows: {silver_count:,}")

# ── Check 2: No negative fares in silver ──────────────────
neg_fares = df_silver.filter(col("fare_amount") <= 0).count()
assert neg_fares == 0, f"❌ Found {neg_fares} negative fares in silver"
print(f"✅ Check 2 PASSED — No negative fares in silver")

# ── Check 3: No zero distance trips in silver ─────────────
zero_dist = df_silver.filter(col("trip_distance") <= 0).count()
assert zero_dist == 0, f"❌ Found {zero_dist} zero distance trips"
print(f"✅ Check 3 PASSED — No zero distance trips")

# ── Check 4: Gold row count matches silver ────────────────
df_gold = spark.read.format("delta").load(GOLD)
gold_count = df_gold.count()
assert gold_count == silver_count, f"❌ Gold ({gold_count}) != Silver ({silver_count})"
print(f"✅ Check 4 PASSED — Gold rows match silver: {gold_count:,}")

# ── Check 5: No NULLs in key fact columns ─────────────────
null_check = df_gold.select(
    count(when(isnull("vendor_id"), True)).alias("null_vendor"),
    count(when(isnull("fare_amount"), True)).alias("null_fare"),
    count(when(isnull("pickup_borough"), True)).alias("null_borough"),
).collect()[0]

assert null_check["null_vendor"] == 0, f"❌ NULL vendor_ids found"
assert null_check["null_fare"] == 0, f"❌ NULL fare amounts found"
print(f"✅ Check 5 PASSED — No NULLs in key columns")

# ── Check 6: Valid vendor IDs only ────────────────────────
invalid_vendors = df_gold.filter(~col("vendor_id").isin([1,2])).count()
assert invalid_vendors == 0, f"❌ Found {invalid_vendors} invalid vendor IDs"
print(f"✅ Check 6 PASSED — All vendor IDs valid")

print("=" * 55)
print("ALL QUALITY CHECKS PASSED ✅")
print("=" * 55)

# COMMAND ----------

