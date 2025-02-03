# Module 2 Homework: Kestra

### Question 1. File size

**Answer: 128.3**

### Question 2. Rendered value

**Answer: green_tripdata_2020-04.csv**

### Question 3. Number of rows (yellow, 2020)

```bash
SELECT COUNT(0)
FROM public.yellow_tripdata
WHERE
    tpep_pickup_datetime >= '2020-01-01' AND tpep_pickup_datetime < '2021-01-01'
    AND tpep_dropoff_datetime >= '2020-01-01' AND tpep_dropoff_datetime < '2021-01-01';
```
**Answer: 29,430,127**

### Question 4. Number of rows (green, 2020)

```bash

SELECT COUNT(*)
FROM public.green_tripdata
WHERE
    EXTRACT(YEAR FROM tpep_pickup_datetime) = 2020;
```
**Answer: 1,734,051**

### Question 5. Number of rows (yellow, March 2021)

```bash

```
**Answer: 1,925,152**

### Question 6. Timezone for trigger

```bash

```
**Answer: X**
