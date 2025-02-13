# Module 3 Homework: Data Warehousing

### Question 1. Count of records for the 2024 Yellow Taxi Data
```sql
SELECT COUNT(0) FROM taxi_rides_ny.external_yellow_tripdata;
```
**Answer: 20,332,093**

### Question 2. Estimated amount of data
```sql
-- Scanning 155.12MB of data
SELECT DISTINCT(PULocationID)
FROM taxi_rides_ny.external_yellow_tripdata_non_partitioned;

-- Scanning ~0 MB of DATA
SELECT DISTINCT(PULocationID)
FROM taxi_rides_ny.external_yellow_tripdata_partitioned;
```
**Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table**

### Question 3. Why are the estimated number of Bytes different?
```sql
SELECT DISTINCT(PULocationID, DOLocationID)
FROM taxi_rides_ny.external_yellow_tripdata_non_partitioned;
```
**Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.**

### Question 4. How many records have a fare_amount of 0?
```sql
SELECT COUNT(0)
FROM taxi_rides_ny.external_yellow_tripdata_non_partitioned
WHERE fare_amount=0;
```
**Answer: 8,333**

### Question 5. The best strategy to make an optimized table in Big Query
```sql
CREATE TABLE taxi_rides_ny.external_yellow_tripdata_optimized_table
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT *
FROM taxi_rides_ny.external_yellow_tripdata_original_table;
```
**Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID**

### Question 6. Estimated processed bytes
```sql
SELECT DISTINCT(VendorID) as vendors
FROM taxi_rides_ny.external_yellow_tripdata_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT(VendorID) as vendors
FROM taxi_rides_ny.external_yellow_tripdata_optimized_table
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```
**Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**

### Question 7. Where is the data for external tables stored?

**Answer: Big Query**

### Question 8. Always clustering

**Answer: False**