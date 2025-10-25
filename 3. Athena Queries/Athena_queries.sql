-- Refresh partitions
MSCK REPAIR TABLE your_table_name;

-- Daily sales summary
SELECT store_id,
       DATE(timestamp) AS date,
       SUM(quantity) AS total_quantity,
       SUM(amount) AS total_sales
FROM your_table_name
GROUP BY store_id, DATE(timestamp);

---------------------------------------

IF PARQUET Data is not there -

-- Create database
CREATE DATABASE IF NOT EXISTS ecommerce_db;

-- CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce_db.ecommerce_transactions (
  transaction_id string,
  store_id string,
  product_id string,
  quantity int,
  amount double,
  payment_type string,
  timestamp timestamp
)
PARTITIONED BY (date date)
STORED AS PARQUET
LOCATION 's3://priyam-tibil-test/ecommerce-curated/';

Followed by Refresh and Daily sales summary