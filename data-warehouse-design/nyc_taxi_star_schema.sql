CREATE DATABASE nyc_taxi_dw;
USE nyc_taxi_dw;

CREATE TABLE dim_vendor (
    vendor_key    INT PRIMARY KEY,
    vendor_name   VARCHAR(100)
);

INSERT INTO dim_vendor VALUES
(1, 'Creative Mobile Technologies'),
(2, 'VeriFone Inc');


CREATE TABLE dim_rate(
	rate_key INT PRIMARY KEY,
    rate_description VARCHAR(50)
	);
    
INSERT INTO dim_rate  VALUES
(1, 'Standard rate'),
(2, 'JFK Airport'),
(3, 'Newark'),
(4, 'Nassau or Westchester'),
(5, 'Negotiated fare'),
(6, 'Group ride');


CREATE TABLE dim_location (
    location_key  INT PRIMARY KEY,
    borough       VARCHAR(50),
    zone_name     VARCHAR(100),
    service_zone  VARCHAR(50)
);


CREATE TABLE dim_date (
    date_key    INT PRIMARY KEY,    -- format YYYYMMDD e.g. 20240105
    full_date   DATE,
    day         INT,
    month       INT,
    month_name  VARCHAR(10),
    quarter     VARCHAR(5),
    year        INT,
    is_weekend  BOOLEAN,
    is_holiday  BOOLEAN
);
ALTER TABLE dim_date
ADD day_of_week VARCHAR(10);

CREATE TABLE dim_time (
    time_key     INT PRIMARY KEY,  -- 143207 (HHMMSS as integer)
    full_time    TIME,
    hour         INT,              -- 0-23
    minute       INT,              -- 0-59
    period       VARCHAR(5),       -- AM / PM
    time_of_day  VARCHAR(20)       -- Morning / Afternoon / Evening / Night
);

CREATE TABLE dim_payment_type(
	code INT PRIMARY KEY,
    description VARCHAR(50)
	);
    
INSERT INTO dim_payment_type VALUES
(1, 'Credit card'),
(2, 'Cash'),
(3, 'No charge'),
(4, 'Dispute'),
(5, 'Unknown'),
(6, 'Voided trip');


CREATE TABLE fact_taxi_trips(
	trip_id INT PRIMARY KEY,
    PULocation_Key INT,
    DOLocation_Key INT,
    pickup_date_Key INT,
    pickup_time_Key INT,
    dropoff_date_Key INT,
    dropoff_time_Key INT,
    payment_Key INT,
    vendor_Key INT,
    ratecode_Key INT,
    passenger_count INT,
    trip_distance DECIMAL(10,3),
    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    store_and_fwd_flag VARCHAR(1),
    FOREIGN KEY(PULocation_Key) REFERENCES dim_location(location_key),
    FOREIGN KEY(DOLocation_Key) REFERENCES dim_location(location_key),
    FOREIGN KEY(pickup_date_Key) REFERENCES dim_date(date_key),
    FOREIGN KEY(pickup_time_Key) REFERENCES dim_time(time_key),
    FOREIGN KEY(dropoff_date_Key) REFERENCES dim_date(date_key),
    FOREIGN KEY(dropoff_time_Key) REFERENCES dim_time(time_key),
    FOREIGN KEY(payment_Key) REFERENCES dim_payment_type(code),
    FOREIGN KEY(vendor_Key) REFERENCES dim_vendor(vendor_key),
    FOREIGN KEY(ratecode_Key) REFERENCES dim_rate(rate_key)
	);
    