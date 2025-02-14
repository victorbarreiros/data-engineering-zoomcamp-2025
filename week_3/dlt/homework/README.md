# Workshop 1 Homework: Ingestion with dlt

### Question 1. The version
```bash
dlt --version
```
**Answer: dlt 1.6.1**

### Question 2. How many tables were created?
```python
print(conn.sql("DESCRIBE").df())
```
**Answer: 4**

### Question 3. The total number of records extracted?
```python
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT COUNT(0)
            FROM ny_taxi_resource;
            """
        )
    print(res)
```
**Answer: 10000**

### Question 4. Average trip duration
```python
with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM ny_taxi_resource;
            """
        )
    print('Trip duration')
    print(res)
```
**Answer: 12.3049**