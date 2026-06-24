CREATE DATABASE food_delivery_dw;
USE food_delivery_dw;
    
CREATE TABLE dim_customer(
	id int primary key,
    customer_name varchar(100),
    customer_city varchar(30),
    customer_email varchar(50)
	);
    
CREATE TABLE dim_restaurant(
	id int primary key,
    restaurant_name varchar(100),
    cuisine_type varchar(30),
    restaurant_city varchar(30)
	);
    
select * from dim_restaurant;
alter table dim_restaurant
rename column res_name to restaurant_name;

alter table dim_restaurant
rename column res_city to restaurant_city;

create table dim_rider(
	id int primary key,
    rider_name varchar(100),
    rider_phone varchar (15)
	);    

create table dim_date(
	date_key int primary key,
    full_date date,
    day int,
    month int,
    month_name varchar(10),
    quarter varchar(10),
    year int,
    is_weekend boolean,
    is_holiday boolean
	);

CREATE TABLE fact_deliveries(
	order_id int PRIMARY KEY,
    customer_key int,
    restaurant_key int,
    rider_key int,
    order_date_key int,
    delivery_date_key int,
    items_ordered int,
    total_amount decimal(10,2),
    delivery_fee decimal(10,2),
    order_status VARCHAR(30),
    payment_method VARCHAR(50),
    FOREIGN KEY(customer_key) references dim_customer(id),
	FOREIGN KEY(restaurant_key) references dim_restaurant(id),
    FOREIGN KEY(rider_key) references dim_rider(id),
    FOREIGN KEY(order_date_key) references dim_date(date_key),
    FOREIGN KEY(delivery_date_key) references dim_date(date_key)
	);    
