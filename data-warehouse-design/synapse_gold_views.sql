SELECT TOP 10 *
FROM OPENROWSET(
    BULK 'https://nyctaxistorageps.dfs.core.windows.net/medallion/gold/star_schema/fact_taxi_trips_enriched/',
    FORMAT = 'DELTA'
) AS fact_trips;

CREATE DATABASE nyc_taxi_gold;

-- Fact table view
CREATE OR ALTER VIEW vw_fact_taxi_trips AS
SELECT *
FROM OPENROWSET(
    BULK 'https://nyctaxistorageps.dfs.core.windows.net/medallion/gold/star_schema/fact_taxi_trips_enriched/',
    FORMAT = 'DELTA'
) AS result;
GO

-- Dimension views
CREATE OR ALTER VIEW vw_dim_vendor AS
SELECT *
FROM OPENROWSET(
    BULK 'https://nyctaxistorageps.dfs.core.windows.net/medallion/gold/star_schema/dim_vendor/',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE OR ALTER VIEW vw_dim_payment AS
SELECT *
FROM OPENROWSET(
    BULK 'https://nyctaxistorageps.dfs.core.windows.net/medallion/gold/star_schema/dim_payment/',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE OR ALTER VIEW vw_dim_rate AS
SELECT *
FROM OPENROWSET(
    BULK 'https://nyctaxistorageps.dfs.core.windows.net/medallion/gold/star_schema/dim_rate/',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE OR ALTER VIEW vw_dim_location AS
SELECT *
FROM OPENROWSET(
    BULK 'https://nyctaxistorageps.dfs.core.windows.net/medallion/gold/star_schema/dim_location/',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE OR ALTER VIEW vw_dim_date AS
SELECT *
FROM OPENROWSET(
    BULK 'https://nyctaxistorageps.dfs.core.windows.net/medallion/gold/star_schema/dim_date/',
    FORMAT = 'DELTA'
) AS result;
GO

CREATE OR ALTER VIEW vw_dim_time AS
SELECT *
FROM OPENROWSET(
    BULK 'https://nyctaxistorageps.dfs.core.windows.net/medallion/gold/star_schema/dim_time/',
    FORMAT = 'DELTA'
) AS result;
GO

PRINT 'All 7 views created successfully!';

USE nyc_taxi_gold;

-- Query 1: Revenue by vendor
SELECT vendor_name,
       COUNT(*) AS total_trips,
       CAST(SUM(fare_amount) AS DECIMAL(15,2)) AS total_revenue,
       CAST(AVG(fare_amount) AS DECIMAL(10,2)) AS avg_fare
FROM vw_fact_taxi_trips
GROUP BY vendor_name
ORDER BY total_revenue DESC;

-- Query 2: Top 10 pickup zones by revenue
SELECT pickup_zone,
       pickup_borough,
       COUNT(*) AS trips,
       CAST(SUM(fare_amount) AS DECIMAL(15,2)) AS revenue
FROM vw_fact_taxi_trips
GROUP BY pickup_zone, pickup_borough
ORDER BY revenue DESC
OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY;

-- Query 3: Join fact with dim_date for day-of-week analysis
SELECT d.day_of_week,
       COUNT(*) AS trips,
       CAST(AVG(f.fare_amount) AS DECIMAL(10,2)) AS avg_fare
FROM vw_fact_taxi_trips f
JOIN vw_dim_date d ON f.pickup_date = CAST(d.full_date AS DATE)
GROUP BY d.day_of_week
ORDER BY trips DESC;