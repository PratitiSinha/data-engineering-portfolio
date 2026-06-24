# Data Warehouse Design

Star schema designs for two business domains, built as part of 
dimensional data modeling for data engineering.

## Schemas

### Food Delivery Star Schema
Models a food delivery company's order data into a star schema with:
- `fact_deliveries` — one row per order (grain)
- `dim_customer` — customer details
- `dim_restaurant` — restaurant and cuisine info
- `dim_rider` — delivery rider details
- `dim_date` — role-playing date dimension (order + delivery date)

### NYC Taxi Star Schema
Models NYC TLC yellow taxi trip data into a production-ready star schema with:
- `fact_taxi_trips` — one row per completed trip (grain)
- `dim_vendor` — taxi vendor (CMT / VeriFone)
- `dim_location` — 263 NYC taxi zones with borough and service zone
- `dim_rate` — rate codes (Standard, JFK, Newark etc.)
- `dim_payment_type` — payment method lookup
- `dim_date` — role-playing date dimension (pickup + dropoff)
- `dim_time` — time dimension with rush hour classification

## Key Concepts Demonstrated
- Star schema design and grain declaration
- Fact vs dimension identification
- Degenerate dimensions
- Role-playing dimensions
- SCD Type 1, 2, 3
- Normalization vs denormalization tradeoffs