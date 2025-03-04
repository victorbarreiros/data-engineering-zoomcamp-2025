# Module 5 Homework: Batch Processing

### Question 1. PySpark version
```python
pyspark.__version__
```
**Answer: 3.3.2 **

### Question 2. Size of partition
```python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option('header', 'true') \
    .parquet('yellow_tripdata_2024-10.parquet')

repartitioned_df = df.repartition(4)
repartitioned_df.write.parquet("output_parquet")
spark.stop()
```
**Answer: 25MB **

### Question 3. Number of trips
```python
df = df.withColumn("tpep_pickup_datetime", to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))

df_filtered = df.filter(
    (year(col("tpep_pickup_datetime")) == 2024) & 
    (month(col("tpep_pickup_datetime")) == 10) &  
    (dayofmonth(col("tpep_pickup_datetime")) == 15) 
)

num_trips = df_filtered.count()

print(f"Number of taxi trips on the 15th of October: {num_trips}")
```
**Answer: 125,567 **

### Question 4. Longest trip
```python
df = df.withColumn(
    "trip_duration_seconds",
    unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))
)

max_trip_duration_seconds = df.select(max(col("trip_duration_seconds"))).collect()[0][0]
max_trip_duration_hours = max_trip_duration_seconds / 3600

print(f"Length of the longest trip in hours: {max_trip_duration_hours}")
```
**Answer: 162 **

### Question 5. User Interface

**Answer: 4040 **

### Question 6. Least frequent pickup location zone
```sql
yellow_trip_df = spark.read.parquet('output_parquet')
yellow_trip_df.createOrReplaceTempView("yellow_tripdata")

zone_lookup_df = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)
zone_lookup_df.createOrReplaceTempView("zone_lookup")

pickup_frequency_df = spark.sql("""
    SELECT z.Zone, COUNT(*) AS pickup_count
    FROM yellow_tripdata y
    JOIN zone_lookup z ON y.PULocationID = z.LocationID
    GROUP BY z.Zone
    ORDER BY pickup_count ASC
""")

least_frequent_zone = pickup_frequency_df.first()["Zone"]
print("The least frequent pickup location zone is:", least_frequent_zone)
```

**Answer: The least frequent pickup location zone is: Governor's Island/Ellis Island/Liberty Island **
