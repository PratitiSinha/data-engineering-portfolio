"""
load_dimensions.py
------------------
Loads three dimension tables into MySQL (nyc_taxi_dw):
  1. dim_location  — from taxi_zone_lookup.csv
  2. dim_date      — generated programmatically (2023-2025)
  3. dim_time      — generated programmatically (every minute of the day)

Run this once to populate your dimension tables.
"""

import os
import csv
import logging
from datetime import date, timedelta
from dotenv import load_dotenv
import mysql.connector

# ── Setup logging ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ── Load credentials ───────────────────────────────────────────────────────
load_dotenv()

DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": "nyc_taxi_dw"
}

# ── Path to your downloaded CSV ────────────────────────────────────────────
TAXI_ZONE_CSV = r"C:\Users\asus\Downloads\taxi_zone_lookup.csv"


# ══════════════════════════════════════════════════════════════════════════
# HELPER
# ══════════════════════════════════════════════════════════════════════════
def get_connection():
    """Return a MySQL connection using DB_CONFIG."""
    return mysql.connector.connect(**DB_CONFIG)


# ══════════════════════════════════════════════════════════════════════════
# 1. LOAD dim_location
# ══════════════════════════════════════════════════════════════════════════
def load_dim_location() -> None:
    """
    Read taxi_zone_lookup.csv and insert into dim_location.
    CSV columns: LocationID, Borough, Zone, service_zone
    """
    logger.info("Loading dim_location from CSV...")

    conn   = get_connection()
    cursor = conn.cursor()

    # Clear existing data so we can re-run safely (idempotent)
    cursor.execute("TRUNCATE TABLE dim_location")

    with open(TAXI_ZONE_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                int(row["LocationID"]),
                row["Borough"],
                row["Zone"],
                row["service_zone"]
            )
            for row in reader
        ]

    sql = """
        INSERT INTO dim_location
            (location_key, borough, zone_name, service_zone)
        VALUES (%s, %s, %s, %s)
    """
    cursor.executemany(sql, rows)
    conn.commit()

    logger.info(f"dim_location: inserted {cursor.rowcount} rows")
    cursor.close()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════
# 2. GENERATE dim_date
# ══════════════════════════════════════════════════════════════════════════
def load_dim_date(
    start: date = date(2023, 1, 1),
    end:   date = date(2025, 12, 31)
) -> None:
    """
    Generate one row per date between start and end (inclusive)
    and insert into dim_date.

    date_key format: YYYYMMDD (e.g. 20240105)
    """
    logger.info(f"Generating dim_date from {start} to {end}...")

    MONTH_NAMES = [
        "", "January","February","March","April","May","June",
        "July","August","September","October","November","December"
    ]
    DAY_NAMES = [
        "Monday","Tuesday","Wednesday",
        "Thursday","Friday","Saturday","Sunday"
    ]

    rows = []
    current = start
    while current <= end:
        date_key   = int(current.strftime("%Y%m%d"))
        month_num  = current.month
        quarter    = f"Q{(month_num - 1) // 3 + 1}"
        day_name   = DAY_NAMES[current.weekday()]
        is_weekend = current.weekday() >= 5  # Saturday=5, Sunday=6

        rows.append((
            date_key,
            current,           # full_date
            current.day,       # day
            month_num,         # month
            MONTH_NAMES[month_num],  # month_name
            quarter,           # quarter
            current.year,      # year
            day_name,          # day_of_week
            is_weekend,        # is_weekend
            False              # is_holiday (simplified — always False)
        ))
        current += timedelta(days=1)

    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE dim_date")

    sql = """
        INSERT INTO dim_date
            (date_key, full_date, day, month, month_name,
             quarter, year, day_of_week, is_weekend, is_holiday)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(sql, rows)
    conn.commit()

    logger.info(f"dim_date: inserted {cursor.rowcount} rows")
    cursor.close()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════
# 3. GENERATE dim_time
# ══════════════════════════════════════════════════════════════════════════
def load_dim_time() -> None:
    """
    Generate one row per minute of the day (1440 rows total)
    and insert into dim_time.

    time_key format: HHMM as integer (e.g. 1432 for 14:32)
    """
    logger.info("Generating dim_time (1440 rows — one per minute)...")

    def get_time_of_day(hour: int) -> str:
        if   5  <= hour < 9:  return "Morning Rush"
        elif 9  <= hour < 12: return "Late Morning"
        elif 12 <= hour < 16: return "Afternoon"
        elif 16 <= hour < 20: return "Evening Rush"
        elif 20 <= hour < 24: return "Night"
        else:                  return "Late Night"

    rows = []
    for hour in range(24):
        for minute in range(60):
            time_key    = hour * 100 + minute  # e.g. 1432
            period      = "AM" if hour < 12 else "PM"
            time_of_day = get_time_of_day(hour)
            full_time   = f"{hour:02d}:{minute:02d}:00"

            rows.append((
                time_key,
                full_time,
                hour,
                minute,
                period,
                time_of_day
            ))

    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE dim_time")

    sql = """
        INSERT INTO dim_time
            (time_key, full_time, hour, minute, period, time_of_day)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor.executemany(sql, rows)
    conn.commit()

    logger.info(f"dim_time: inserted {cursor.rowcount} rows")
    cursor.close()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════
def run_all() -> None:
    logger.info("=" * 60)
    logger.info("Dimension Loader — START")
    logger.info("=" * 60)

    try:
        load_dim_location()
        load_dim_date()
        load_dim_time()

        logger.info("=" * 60)
        logger.info("All dimensions loaded successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Dimension loading FAILED: {e}")
        raise


if __name__ == "__main__":
    run_all()