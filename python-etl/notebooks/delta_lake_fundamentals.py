# Databricks notebook source
# Cell 1 — Create sample data and save as Delta table
# Using Unity Catalog (modern Databricks approach)

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Sample NYC taxi data
data = [
    (1, "2024-01-05", "2024-01-05", 2, 3.5,  14.50, 2.50, 17.00, 1),
    (2, "2024-01-05", "2024-01-05", 1, 1.2,  8.00,  1.50, 9.50,  2),
    (3, "2024-01-06", "2024-01-06", 3, 7.8,  28.00, 4.00, 32.00, 1),
    (4, "2024-01-06", "2024-01-06", 1, 2.1,  11.00, 2.00, 13.00, 1),
    (5, "2024-01-07", "2024-01-07", 2, 5.3,  20.00, 3.00, 23.00, 2),
]

schema = StructType([
    StructField("trip_id",         IntegerType(), False),
    StructField("pickup_date",     StringType(),  True),
    StructField("dropoff_date",    StringType(),  True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance",   DoubleType(),  True),
    StructField("fare_amount",     DoubleType(),  True),
    StructField("tip_amount",      DoubleType(),  True),
    StructField("total_amount",    DoubleType(),  True),
    StructField("vendor_id",       IntegerType(), True),
])

df = spark.createDataFrame(data, schema)

# Save as Delta table using SQL warehouse (no file path needed)
spark.sql("CREATE DATABASE IF NOT EXISTS taxi_learning")
spark.sql("DROP TABLE IF EXISTS taxi_learning.taxi_trips")

df.write.format("delta") \
  .mode("overwrite") \
  .saveAsTable("taxi_learning.taxi_trips")

print("✅ Delta table created successfully!")
df.show()


# COMMAND ----------

# Cell 2 — Read Delta table and check its history

# Read the Delta table
df_read = spark.table("taxi_learning.taxi_trips")
print("📖 Reading from Delta table:")
df_read.show()

# Check Delta table history — this is the transaction log
print("\n📋 Delta table history (transaction log):")
spark.sql("DESCRIBE HISTORY taxi_learning.taxi_trips").show(truncate=False)

# COMMAND ----------



# COMMAND ----------

# Cell 3 — Update data (creates version 1 in transaction log)

from pyspark.sql.functions import col

# Apply a 10% fare increase to all vendor_id = 1 trips
spark.sql("""
    UPDATE taxi_learning.taxi_trips
    SET fare_amount = fare_amount * 1.10,
        total_amount = total_amount * 1.10
    WHERE vendor_id = 1
""")

print("✅ Update complete — fare increased 10% for vendor 1")

# Show updated data
spark.table("taxi_learning.taxi_trips").show()

# Check history again — should now show version 1
print("\n📋 Updated transaction log:")
spark.sql("""
    SELECT version, timestamp, operation, operationMetrics
    FROM (DESCRIBE HISTORY taxi_learning.taxi_trips)
""").show(truncate=False)

# COMMAND ----------

# Cell 4 — Time Travel (query previous versions)

print("📌 CURRENT version (version 1 — after 10% fare increase):")
spark.table("taxi_learning.taxi_trips").select(
    "trip_id", "vendor_id", "fare_amount", "total_amount"
).show()

print("⏪ VERSION 0 (before the update — original fares):")
spark.sql("""
    SELECT trip_id, vendor_id, fare_amount, total_amount
    FROM taxi_learning.taxi_trips VERSION AS OF 0
""").show()

print("🔍 Comparing fare_amount changes:")
spark.sql("""
    SELECT 
        current.trip_id,
        current.vendor_id,
        original.fare_amount AS original_fare,
        current.fare_amount  AS current_fare,
        ROUND(current.fare_amount - original.fare_amount, 2) AS difference
    FROM taxi_learning.taxi_trips current
    JOIN (
        SELECT trip_id, fare_amount 
        FROM taxi_learning.taxi_trips VERSION AS OF 0
    ) original
    ON current.trip_id = original.trip_id
    ORDER BY current.trip_id
""").show()

# COMMAND ----------

# Cell 5 — Schema Evolution (add a new column)
from pyspark.sql.types import * 

print("📋 BEFORE schema evolution:")
print("Current columns:", spark.table("taxi_learning.taxi_trips").columns)

# Add new data WITH an extra column (payment_method)
# This would FAIL with regular Parquet — Delta handles it gracefully
new_data = [
    (6, "2024-01-08", "2024-01-08", 1, 4.2, 16.00, 3.00, 19.00, 1, "Credit Card"),
    (7, "2024-01-08", "2024-01-08", 2, 2.8, 12.00, 2.00, 14.00, 2, "Cash"),
]

new_schema = StructType([
    StructField("trip_id",          IntegerType(), False),
    StructField("pickup_date",      StringType(),  True),
    StructField("dropoff_date",     StringType(),  True),
    StructField("passenger_count",  IntegerType(), True),
    StructField("trip_distance",    DoubleType(),  True),
    StructField("fare_amount",      DoubleType(),  True),
    StructField("tip_amount",       DoubleType(),  True),
    StructField("total_amount",     DoubleType(),  True),
    StructField("vendor_id",        IntegerType(), True),
    StructField("payment_method",   StringType(),  True),  # NEW column
])

df_new = spark.createDataFrame(new_data, new_schema)

# mergeSchema=True tells Delta to add the new column automatically
df_new.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("taxi_learning.taxi_trips")

print("\n✅ Schema evolution complete!")
print("New columns:", spark.table("taxi_learning.taxi_trips").columns)

print("\n📊 Table after schema evolution:")
spark.sql("""
    SELECT * FROM taxi_learning.taxi_trips
    ORDER BY trip_id DESC
""").show()

# COMMAND ----------

# Fix — remove duplicates cleanly using ROW_NUMBER
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Read current table
df_current = spark.table("taxi_learning.taxi_trips")

# Keep only first occurrence of each trip_id
window = Window.partitionBy("trip_id").orderBy("trip_id")
df_deduped = df_current.withColumn("rn", row_number().over(window)) \
                        .filter("rn = 1") \
                        .drop("rn")

# Overwrite table with deduplicated data
df_deduped.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("taxi_learning.taxi_trips")

print("✅ Duplicates removed!")
print(f"Row count: {df_deduped.count()}")
spark.sql("SELECT * FROM taxi_learning.taxi_trips ORDER BY trip_id DESC").show()

# COMMAND ----------

# Cell 6 — View complete transaction history

print("📋 Complete Delta table history:")
spark.sql("""
    SELECT version, timestamp, operation, operationMetrics
    FROM (DESCRIBE HISTORY taxi_learning.taxi_trips)
    ORDER BY version DESC
""").show(truncate=False)

# COMMAND ----------

# Cell 7 — OPTIMIZE and VACUUM

# OPTIMIZE: compact small files into larger ones for faster queries
print("⚡ Running OPTIMIZE...")
spark.sql("OPTIMIZE taxi_learning.taxi_trips")
print("✅ OPTIMIZE complete")

# DESCRIBE DETAIL: show table metadata
print("\n📊 Table details:")
spark.sql("DESCRIBE DETAIL taxi_learning.taxi_trips").select(
    "name", "format", "numFiles", "sizeInBytes", "location"
).show(truncate=False)

# VACUUM: clean up old files (default 7 day retention)
print("\n🧹 Running VACUUM (dry run — shows what would be deleted):")
spark.sql("VACUUM taxi_learning.taxi_trips DRY RUN").show(truncate=False)