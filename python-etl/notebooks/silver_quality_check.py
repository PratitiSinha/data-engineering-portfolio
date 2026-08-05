# Databricks notebook source
# silver_quality_check
# Validates silver layer after bronze_to_silver transformation
# Fails fast with clear error messages if any check fails

import os
from pyspark.sql.functions import col, count, when, isnull, year, month

storage_account = "nyctaxistorageps"
container = "medallion"
storage_key = os.environ.get("AZURE_STORAGE_KEY")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

BASE   = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
SILVER = f"{BASE}/silver/cleaned/yellow_taxi/"

print("=" * 55)
print("SILVER LAYER QUALITY CHECKS")
print("=" * 55)

df = spark.read.format("delta").load(SILVER)
total = df.count()

# ── Check 1: Row count ─────────────────────────────────────
assert total > 2000000, f"❌ Row count too low: {total:,}"
print(f"✅ Check 1 PASSED — Row count: {total:,}")

# ── Check 2: No negative fares ────────────────────────────
neg_fares = df.filter(col("fare_amount") <= 0).count()
assert neg_fares == 0, f"❌ Found {neg_fares} negative fares"
print(f"✅ Check 2 PASSED — No negative fares")

# ── Check 3: No zero distance trips ───────────────────────
zero_dist = df.filter(col("trip_distance") <= 0).count()
assert zero_dist == 0, f"❌ Found {zero_dist} zero distance trips"
print(f"✅ Check 3 PASSED — No zero distance trips")

# ── Check 4: No zero passengers ───────────────────────────
zero_pass = df.filter(col("passenger_count") <= 0).count()
assert zero_pass == 0, f"❌ Found {zero_pass} zero passenger trips"
print(f"✅ Check 4 PASSED — No zero passengers")

# ── Check 5: No NULL passenger_count ──────────────────────
null_pass = df.filter(isnull(col("passenger_count"))).count()
assert null_pass == 0, f"❌ Found {null_pass} NULL passenger counts"
print(f"✅ Check 5 PASSED — No NULL passenger counts")

# ── Check 6: Only correct year/month ──────────────────────
wrong_year = df.filter(col("pickup_year") != 2024).count()
assert wrong_year == 0, f"❌ Found {wrong_year} records with wrong year"
print(f"✅ Check 6 PASSED — All records are 2024")

wrong_month = df.filter(col("pickup_month") != 1).count()
assert wrong_month == 0, f"❌ Found {wrong_month} records with wrong month"
print(f"✅ Check 7 PASSED — All records are January")

# ── Check 7: No invalid timestamps ────────────────────────
invalid_ts = df.filter(
    col("pickup_datetime") >= col("dropoff_datetime")
).count()
assert invalid_ts == 0, f"❌ Found {invalid_ts} invalid timestamps"
print(f"✅ Check 8 PASSED — All timestamps valid")

# ── Check 8: trip_duration_mins is positive ────────────────
neg_duration = df.filter(col("trip_duration_mins") <= 0).count()
assert neg_duration == 0, f"❌ Found {neg_duration} negative durations"
print(f"✅ Check 9 PASSED — All trip durations positive")

# ── Summary ────────────────────────────────────────────────
print("=" * 55)
print(f"ALL SILVER CHECKS PASSED ✅")
print(f"Silver rows ready for gold: {total:,}")
print("=" * 55)

# COMMAND ----------

