# Module 1 Homework: Docker & SQL

### Question 1. Understanding docker first run

```bash
docker run -it --entrypoint bash python:3.12.8

pip --version
```
**Answer: 24.3.1**

### Question 2. Understanding Docker networking and docker-compose

**Answer: db:5432**

### Question 3. Trip Segmentation Count

1. Up to 1 mile

```sql
SELECT COUNT(*) 
FROM green_taxi_data
WHERE 
    lpep_pickup_datetime >= '2019-10-01 00:00:00' 
    AND lpep_pickup_datetime < '2019-11-01 00:00:00' 
    AND lpep_dropoff_datetime >= '2019-10-01' 
    AND lpep_dropoff_datetime < '2019-11-01' 
    AND trip_distance <= 1;
```
**Answer: 104.802**

2. In between 1 (exclusive) and 3 miles (inclusive),

```sql
SELECT COUNT(*) 
FROM green_taxi_data
WHERE 
    lpep_pickup_datetime >= '2019-10-01 00:00:00' 
    AND lpep_pickup_datetime < '2019-11-01 00:00:00' 
    AND lpep_dropoff_datetime >= '2019-10-01' 
    AND lpep_dropoff_datetime < '2019-11-01' 
    AND trip_distance > 1 
    AND trip_distance <= 3;
```
**Answer: 198.924**
3. In between 3 (exclusive) and 7 miles (inclusive),

```sql
SELECT COUNT(*) 
FROM green_taxi_data
WHERE 
    lpep_pickup_datetime >= '2019-10-01 00:00:00' 
    AND lpep_pickup_datetime < '2019-11-01 00:00:00' 
    AND lpep_dropoff_datetime >= '2019-10-01' 
    AND lpep_dropoff_datetime < '2019-11-01' 
    AND trip_distance > 3 
    AND trip_distance <= 7;
```
**Answer: 109.603**

4. In between 7 (exclusive) and 10 miles (inclusive),

```sql
SELECT COUNT(*) 
FROM green_taxi_data
WHERE 
    lpep_pickup_datetime >= '2019-10-01 00:00:00' 
    AND lpep_pickup_datetime < '2019-11-01 00:00:00' 
    AND lpep_dropoff_datetime >= '2019-10-01' 
    AND lpep_dropoff_datetime < '2019-11-01' 
    AND trip_distance > 7 
    AND trip_distance <= 10;
```
**Answer: 27.678**
5. Over 10 miles

```sql
SELECT COUNT(*) 
FROM green_taxi_data
WHERE 
    lpep_pickup_datetime >= '2019-10-01 00:00:00' 
    AND lpep_pickup_datetime < '2019-11-01 00:00:00' 
    AND lpep_dropoff_datetime >= '2019-10-01' 
    AND lpep_dropoff_datetime < '2019-11-01' 
    AND trip_distance > 10;
```
**Answer: 35.189**

### Question 4. Longest trip for each day

```sql
SELECT 
	DATE(lpep_pickup_datetime) AS trip_date
FROM green_taxi_data
ORDER BY trip_distance DESC
LIMIT 1;
```
**Answer: 2019-10-31**

### Question 5. Three biggest pickup zones

```sql
SELECT 
	z."Borough", 
	z."Zone", 
	z."LocationID", 
	SUM(g.total_amount) AS total_amount
FROM green_taxi_data g
JOIN zones z 
	ON g."PULocationID" = z."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY z."Borough", z."LocationID", z."Zone"
HAVING SUM(g.total_amount) > 13000
ORDER BY total_amount DESC;
```
**Answer: East Harlem North, East Harlem South, Morningside Heights**

### Question 6. Largest tip
```sql
SELECT z2."Zone", z2."LocationID" 
	FROM green_taxi_data g
	JOIN zones z1 
		ON g."PULocationID" = z1."LocationID"
	JOIN zones z2 
		ON g."DOLocationID" = z2."LocationID"
WHERE 
    z1."Zone" = 'East Harlem North'
ORDER BY g.tip_amount DESC 
LIMIT 1;
```
**Answer: JFK Airport**

