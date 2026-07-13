# Databricks notebook source
# Cell 1 — Create dim_customer table (initial load)
# This simulates your dimension table as it exists today

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Initial customer data
data = [
    (1, "Rahul Sharma",  "Gurugram", "Engineering", "2020-01-01", "9999-12-31", True),
    (2, "Priya Nair",    "Mumbai",   "Marketing",   "2019-06-15", "9999-12-31", True),
    (3, "Ravi Kapoor",   "Bangalore","Sales",        "2021-03-10", "9999-12-31", True),
    (4, "Sneha Mehta",   "Delhi",    "Finance",      "2022-07-20", "9999-12-31", True),
    (5, "Aditya Kumar",  "Chennai",  "Engineering",  "2023-01-05", "9999-12-31", True),
]

schema = StructType([
    StructField("surrogate_key",  IntegerType(), False),
    StructField("name",           StringType(),  True),
    StructField("city",           StringType(),  True),
    StructField("department",     StringType(),  True),
    StructField("start_date",     StringType(),  True),
    StructField("end_date",       StringType(),  True),
    StructField("is_current",     BooleanType(), True),
])

df = spark.createDataFrame(data, schema)

# Save as Delta table
spark.sql("CREATE DATABASE IF NOT EXISTS scd_learning")
spark.sql("DROP TABLE IF EXISTS scd_learning.dim_customer")

df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("scd_learning.dim_customer")

print("✅ dim_customer created with initial data!")
df.show()

# COMMAND ----------

# Cell 2 — Create staging table (incoming changes from source)
# This is what arrives in your pipeline today

staging_data = [
    # Rahul moved from Gurugram to Mumbai — city changed
    (1, "Rahul Sharma",  "Mumbai",    "Engineering"),
    # Priya's department changed — Marketing to HR
    (2, "Priya Nair",    "Mumbai",    "HR"),
    # Ravi — no change (same data)
    (3, "Ravi Kapoor",   "Bangalore", "Sales"),
    # Brand new customer — not in dim_customer yet
    (6, "Neha Joshi",    "Pune",      "Marketing"),
]

staging_schema = StructType([
    StructField("customer_id",  IntegerType(), False),
    StructField("name",         StringType(),  True),
    StructField("city",         StringType(),  True),
    StructField("department",   StringType(),  True),
])

df_staging = spark.createDataFrame(staging_data, staging_schema)

spark.sql("DROP TABLE IF EXISTS scd_learning.staging_customers")
df_staging.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("scd_learning.staging_customers")

print("✅ Staging table created!")
print("Changes incoming:")
print("  → Rahul: city changed Gurugram → Mumbai")
print("  → Priya: department changed Marketing → HR")
print("  → Ravi:  no change")
print("  → Neha:  NEW customer")
df_staging.show()

# COMMAND ----------

# Cell 3 — SCD Type 2 implementation using Delta MERGE
# Step 1: Close existing records that have changed
# Step 2: Insert new records (both updates and new customers)

from datetime import date

today = date.today().strftime("%Y-%m-%d")

# ── STEP 1: Close old records that have changed ─────────────────────────
# Find customers where city OR department has changed
# Set end_date = today, is_current = FALSE
spark.sql(f"""
    MERGE INTO scd_learning.dim_customer AS target
    USING scd_learning.staging_customers AS source
    ON target.surrogate_key = source.customer_id
    AND target.is_current = TRUE
    WHEN MATCHED AND (
        target.city       <> source.city OR
        target.department <> source.department
    )
    THEN UPDATE SET
        target.end_date    = '{today}',
        target.is_current  = FALSE
""")

print("✅ Step 1 complete — old records closed")
spark.sql("SELECT * FROM scd_learning.dim_customer ORDER BY surrogate_key").show()

# COMMAND ----------

# Cell 4 — Insert new rows for changed records + brand new customers
# This completes the SCD Type 2 pattern

from datetime import date
today = date.today().strftime("%Y-%m-%d")

# Get max surrogate key to generate new ones
max_key = spark.sql("SELECT MAX(surrogate_key) FROM scd_learning.dim_customer").collect()[0][0]

print(f"Current max surrogate key: {max_key}")

# Build new rows to insert:
# 1. Updated versions of Rahul and Priya (with new city/dept)
# 2. Brand new customer Neha
spark.sql(f"""
    INSERT INTO scd_learning.dim_customer
    SELECT
        ROW_NUMBER() OVER (ORDER BY source.customer_id) + {max_key} AS surrogate_key,
        source.name,
        source.city,
        source.department,
        '{today}'       AS start_date,
        '9999-12-31'    AS end_date,
        TRUE            AS is_current
    FROM scd_learning.staging_customers source
    LEFT JOIN scd_learning.dim_customer target
        ON source.customer_id = target.surrogate_key
        AND target.is_current = FALSE
        AND target.end_date = '{today}'
    WHERE target.surrogate_key IS NOT NULL
       OR NOT EXISTS (
            SELECT 1 FROM scd_learning.dim_customer d
            WHERE d.surrogate_key = source.customer_id
       )
""")

print("✅ Step 2 complete — new records inserted!")
print("\n📊 Final dim_customer state:")
spark.sql("""
    SELECT surrogate_key, name, city, department,
           start_date, end_date, is_current
    FROM scd_learning.dim_customer
    ORDER BY name, start_date
""").show()

# COMMAND ----------

# Cell 5 — OPTIMIZE with Z-ordering
# Z-ordering co-locates related data in same files
# Dramatically speeds up queries that filter on these columns

print("Before OPTIMIZE — table details:")
spark.sql("DESCRIBE DETAIL scd_learning.dim_customer") \
    .select("numFiles", "sizeInBytes") \
    .show()

# OPTIMIZE with Z-order on columns most commonly filtered
spark.sql("""
    OPTIMIZE scd_learning.dim_customer
    ZORDER BY (is_current, name)
""")

print("✅ OPTIMIZE + Z-ordering complete!")
print("After OPTIMIZE — table details:")
spark.sql("DESCRIBE DETAIL scd_learning.dim_customer") \
    .select("numFiles", "sizeInBytes") \
    .show()

print("\n📋 Final transaction history:")
spark.sql("""
    SELECT version, timestamp, operation
    FROM (DESCRIBE HISTORY scd_learning.dim_customer)
    ORDER BY version DESC
""").show(truncate=False)
