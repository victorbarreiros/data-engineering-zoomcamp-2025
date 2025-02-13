-- Query public available table
SELECT station_id, name FROM
    bigquery-public-data.new_york_citibike.citibike_stations
LIMIT 100;


-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi_rides_ny.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_yellow_taxi_us/yellow_tripdata_2024-0*.parquet', 'gs://de_zoomcamp_yellow_taxi_us/yellow_tripdata_2024-06.parquet']
);

-- Check yello trip data
SELECT * FROM taxi_rides_ny.external_yellow_tripdata limit 10;
SELECT COUNT(0) FROM taxi_rides_ny.external_yellow_tripdata;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE taxi_rides_ny.external_yellow_tripdata_non_partitioned AS
SELECT * FROM taxi_rides_ny.external_yellow_tripdata;


-- Create a partitioned table from external table
CREATE OR REPLACE TABLE taxi_rides_ny.external_yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM taxi_rides_ny.external_yellow_tripdata;

-- Impact of partition
-- Scanning 155.12MB of data
SELECT DISTINCT(PULocationID)
FROM taxi_rides_ny.external_yellow_tripdata_non_partitioned;

-- Scanning ~0 MB of DATA
SELECT DISTINCT(PULocationID)
FROM taxi_rides_ny.external_yellow_tripdata_partitioned;

SELECT COUNT(0)
FROM taxi_rides_ny.external_yellow_tripdata_non_partitioned
WHERE fare_amount=0;


CREATE TABLE taxi_rides_ny.external_yellow_tripdata_optimized_table
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID                       
AS
SELECT *
FROM taxi_rides_ny.external_yellow_tripdata;

SELECT DISTINCT(VendorID) as vendors
FROM taxi_rides_ny.external_yellow_tripdata_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';


SELECT DISTINCT(VendorID) as vendors
FROM taxi_rides_ny.external_yellow_tripdata_optimized_table
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `nytaxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE taxi_rides_ny.external_yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi_rides_ny.external_yellow_tripdata;

-- Query scans 1.1 GB
SELECT count(*) as trips
FROM taxi_rides_ny.external_yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query scans 864.5 MB
SELECT count(*) as trips
FROM taxi_rides_ny.external_yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;