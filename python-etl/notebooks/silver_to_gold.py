# Databricks notebook source
# Cell 1 — silver_to_gold configuration
# Reads silver layer, builds complete star schema, writes to gold

import os
from pyspark.sql.functions import broadcast, col, monotonically_increasing_id
from pyspark.sql.types import *
import requests

storage_account = "nyctaxistorageps"
container = "medallion"
storage_key = os.environ.get("AZURE_STORAGE_KEY")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

BASE         = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
SILVER       = f"{BASE}/silver/cleaned/yellow_taxi/"
GOLD_BASE    = f"{BASE}/gold/star_schema/"
GOLD_FACT    = f"{GOLD_BASE}fact_taxi_trips_enriched/"

print("✅ Configuration complete!")
print(f"Silver: {SILVER}")
print(f"Gold:   {GOLD_BASE}")

# COMMAND ----------

# Cell 2 — Read silver layer

df_silver = spark.read.format("delta").load(SILVER)
print(f"✅ Silver data loaded: {df_silver.count():,} rows")
print(f"Columns: {len(df_silver.columns)}")
df_silver.printSchema()

# COMMAND ----------

# Cell 3 — Create small dimension DataFrames
# These are hardcoded lookup tables — perfect for broadcast joins

# dim_vendor (2 rows)
dim_vendor = spark.createDataFrame([
    (1, "Creative Mobile Technologies"),
    (2, "VeriFone Inc"),
], ["vendor_id", "vendor_name"])

# dim_payment (6 rows)
dim_payment = spark.createDataFrame([
    (1, "Credit Card"),(2, "Cash"),(3, "No Charge"),
    (4, "Dispute"),(5, "Unknown"),(6, "Voided Trip"),
], ["payment_type", "payment_name"])

# dim_rate (6 rows)
dim_rate = spark.createDataFrame([
    (1, "Standard Rate"),(2, "JFK"),(3, "Newark"),
    (4, "Nassau/Westchester"),(5, "Negotiated Fare"),(6, "Group Ride"),
], ["rate_code_id", "rate_name"])

print("✅ Dimension DataFrames created!")
print(f"   dim_vendor:  {dim_vendor.count()} rows")
print(f"   dim_payment: {dim_payment.count()} rows")
print(f"   dim_rate:    {dim_rate.count()} rows")

# COMMAND ----------

# Cell 4 — Load dim_location from NYC TLC public URL
# 265 NYC taxi zones with borough and service zone info

import requests

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
response = requests.get(url)
content = response.content.decode("utf-8")

rows = []
lines = content.strip().split("\n")
for line in lines[1:]:  # skip header
    parts = line.strip().split(",")
    if len(parts) >= 4:
        rows.append((
            int(parts[0]),
            parts[1].strip('"'),
            parts[2].strip('"'),
            parts[3].strip('"'),
        ))

schema = StructType([
    StructField("location_id",  IntegerType(), True),
    StructField("borough",      StringType(),  True),
    StructField("zone_name",    StringType(),  True),
    StructField("service_zone", StringType(),  True),
])

dim_location = spark.createDataFrame(rows, schema)
print(f"✅ dim_location loaded: {len(rows)} zones")
dim_location.show(3)

# COMMAND ----------

# Cell 5 — Build enriched fact table
# Joins silver with all dimensions including role-playing dim_location

# Alias dim_location for each role
dim_pickup  = dim_location.alias("pickup")
dim_dropoff = dim_location.alias("dropoff")

fact_enriched = df_silver \
    .join(broadcast(dim_vendor),  on="vendor_id",    how="left") \
    .join(broadcast(dim_payment), on="payment_type", how="left") \
    .join(broadcast(dim_rate),    on="rate_code_id", how="left") \
    .join(broadcast(dim_pickup),
          df_silver.pickup_location_id == col("pickup.location_id"), "left") \
    .withColumnRenamed("borough",      "pickup_borough") \
    .withColumnRenamed("zone_name",    "pickup_zone") \
    .withColumnRenamed("service_zone", "pickup_service_zone") \
    .drop("location_id") \
    .join(broadcast(dim_dropoff),
          df_silver.dropoff_location_id == col("dropoff.location_id"), "left") \
    .withColumnRenamed("borough",      "dropoff_borough") \
    .withColumnRenamed("zone_name",    "dropoff_zone") \
    .withColumnRenamed("service_zone", "dropoff_service_zone") \
    .drop("location_id") \
    .select(
        monotonically_increasing_id().alias("trip_surrogate_key"),
        col("vendor_id"), col("vendor_name"),
        col("pickup_location_id"), col("pickup_borough"), col("pickup_zone"), col("pickup_service_zone"),
        col("dropoff_location_id"), col("dropoff_borough"), col("dropoff_zone"), col("dropoff_service_zone"),
        col("rate_code_id"), col("rate_name"),
        col("payment_type"), col("payment_name"),
        col("pickup_datetime"), col("dropoff_datetime"),
        col("pickup_date"), col("pickup_year"), col("pickup_month"),
        col("trip_duration_mins"), col("passenger_count"),
        col("trip_distance"), col("fare_amount"),
        col("tip_amount"), col("tolls_amount"),
        col("total_amount"), col("congestion_surcharge"), col("airport_fee")
    )

print(f"✅ fact_enriched built!")
print(f"   Rows: {fact_enriched.count():,}")
print(f"   Columns: {len(fact_enriched.columns)}")
fact_enriched.select("vendor_name", "pickup_borough", "dropoff_borough", "fare_amount").show(3)

# COMMAND ----------

# Cell 6 — Write enriched fact table to gold layer

print("Writing fact_taxi_trips_enriched to gold layer...")

fact_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month") \
    .save(GOLD_FACT)

print(f"✅ fact_taxi_trips_enriched written!")
print(f"Location: {GOLD_FACT}")

# Verify
df_verify = spark.read.format("delta").load(GOLD_FACT)
print(f"Verification — rows: {df_verify.count():,}")

# COMMAND ----------

# Cell 7 — Write dimension tables to gold layer

print("Writing dimension tables to gold layer...")

dims = {
    "dim_vendor":   dim_vendor,
    "dim_payment":  dim_payment,
    "dim_rate":     dim_rate,
    "dim_location": dim_location,
}

for name, df in dims.items():
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_BASE}{name}/")
    print(f"✅ {name} written: {df.count()} rows")

print("\n✅ All dimension tables written!")

# COMMAND ----------

# Cell 8 — Generate and write dim_date (2023-2025)

from datetime import date, timedelta

print("Generating dim_date...")

MONTH_NAMES = ["","January","February","March","April","May","June",
               "July","August","September","October","November","December"]
DAY_NAMES = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

rows = []
current = date(2023, 1, 1)
end = date(2025, 12, 31)

while current <= end:
    month_num = current.month
    rows.append((
        int(current.strftime("%Y%m%d")),
        str(current),
        current.day,
        month_num,
        MONTH_NAMES[month_num],
        f"Q{(month_num - 1) // 3 + 1}",
        current.year,
        DAY_NAMES[current.weekday()],
        current.weekday() >= 5,
        False
    ))
    current += timedelta(days=1)

schema = StructType([
    StructField("date_key",    IntegerType(), False),
    StructField("full_date",   StringType(),  True),
    StructField("day",         IntegerType(), True),
    StructField("month",       IntegerType(), True),
    StructField("month_name",  StringType(),  True),
    StructField("quarter",     StringType(),  True),
    StructField("year",        IntegerType(), True),
    StructField("day_of_week", StringType(),  True),
    StructField("is_weekend",  BooleanType(), True),
    StructField("is_holiday",  BooleanType(), True),
])

dim_date = spark.createDataFrame(rows, schema)

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_BASE}dim_date/")

print(f"✅ dim_date written: {len(rows)} rows")
dim_date.show(3)

# COMMAND ----------

# Cell 9 — Generate and write dim_time (1440 rows — one per minute)

print("Generating dim_time...")

def get_time_of_day(hour):
    if   5  <= hour < 9:  return "Morning Rush"
    elif 9  <= hour < 12: return "Late Morning"
    elif 12 <= hour < 16: return "Afternoon"
    elif 16 <= hour < 20: return "Evening Rush"
    elif 20 <= hour < 24: return "Night"
    else:                  return "Late Night"

rows = []
for h in range(24):
    for m in range(60):
        rows.append((
            h * 100 + m,
            f"{h:02d}:{m:02d}:00",
            h,
            m,
            "AM" if h < 12 else "PM",
            get_time_of_day(h)
        ))

schema = StructType([
    StructField("time_key",    IntegerType(), False),
    StructField("full_time",   StringType(),  True),
    StructField("hour",        IntegerType(), True),
    StructField("minute",      IntegerType(), True),
    StructField("period",      StringType(),  True),
    StructField("time_of_day", StringType(),  True),
])

dim_time = spark.createDataFrame(rows, schema)

dim_time.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{GOLD_BASE}dim_time/")

print(f"✅ dim_time written: {len(rows)} rows")
dim_time.show(3)

# COMMAND ----------

# Cell 10 — Verify complete star schema in gold layer

print("📊 COMPLETE STAR SCHEMA — Gold Layer Inventory")
print("=" * 55)

tables = {
    "fact_taxi_trips_enriched": GOLD_FACT,
    "dim_vendor":               f"{GOLD_BASE}dim_vendor/",
    "dim_payment":              f"{GOLD_BASE}dim_payment/",
    "dim_rate":                 f"{GOLD_BASE}dim_rate/",
    "dim_location":             f"{GOLD_BASE}dim_location/",
    "dim_date":                 f"{GOLD_BASE}dim_date/",
    "dim_time":                 f"{GOLD_BASE}dim_time/",
}

total_rows = 0
for name, path in tables.items():
    df = spark.read.format("delta").load(path)
    rows = df.count()
    cols = len(df.columns)
    total_rows += rows
    print(f"  {name:<35} {rows:>10,} rows  {cols:>3} cols")

print("=" * 55)
print(f"  {'TOTAL':<35} {total_rows:>10,} rows")
print("\n✅ Star schema complete and verified!")

# COMMAND ----------

