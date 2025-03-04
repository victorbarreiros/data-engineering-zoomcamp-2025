# Module 4 Homework: Analytics Engineering

### Question 1. Model resolution 
```sql
select * 
from myproject.my_nyc_tripdata.ext_green_taxi
```
**Answer: select * from myproject.my_nyc_tripdata.ext_green_taxi **

### Question 2. Change the query
```sql
select *
    from {{ ref('fact_taxi_trips') }}
    where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
order by pickup_datetime desc
```
**Answer: Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY**

### Question 3. Lineage
```
This command only materializes staging models and does not include fct_taxi_monthly_zone_revenue
```
**Answer: dbt run --select models/staging/+**

### Question 4. Macros and Jinja
```
- Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile: 

Is he only False option: The macro provides a fallback to DBT_BIGQUERY_TARGET_DATASET if DBT_BIGQUERY_STAGING_DATASET is not set. It will not fail if DBT_BIGQUERY_STAGING_DATASET is missing.
```
**Answer: 1, 3, 4, 5**

### Question 5. Taxi Quarterly Revenue Growth
```sql
WITH quarterly_revenue AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        CONCAT(EXTRACT(YEAR FROM pickup_datetime), '/Q', EXTRACT(QUARTER FROM pickup_datetime)) AS year_quarter,
        SUM(total_amount) AS quarterly_revenue
    FROM
        `datawarehousing-de-zoomcamp.dbt_vbarreiros.fct_taxi_trips`
    GROUP BY
        service_type,
        year,
        quarter,
        year_quarter
),
yoy_growth AS (
    SELECT
        curr.service_type,
        curr.year_quarter,
        curr.quarterly_revenue AS current_quarter_revenue,
        prev.quarterly_revenue AS previous_quarter_revenue,
        ROUND(
            ((curr.quarterly_revenue - prev.quarterly_revenue) / prev.quarterly_revenue * 100),
            2
        ) AS yoy_growth_percent
    FROM
        quarterly_revenue curr
    LEFT JOIN
        quarterly_revenue prev
    ON
        curr.service_type = prev.service_type
        AND curr.quarter = prev.quarter
        AND curr.year = prev.year + 1
),
ranked_growth AS (
    SELECT
        service_type,
        year_quarter,
        yoy_growth_percent,
        RANK() OVER (PARTITION BY service_type ORDER BY yoy_growth_percent DESC) AS best_rank,
        RANK() OVER (PARTITION BY service_type ORDER BY yoy_growth_percent ASC) AS worst_rank
    FROM
        yoy_growth
    WHERE
        year_quarter LIKE '2020%'
)
SELECT
    service_type,
    MAX(CASE WHEN best_rank = 1 THEN year_quarter END) AS best_quarter,
    MAX(CASE WHEN worst_rank = 1 THEN year_quarter END) AS worst_quarter
FROM
    ranked_growth
GROUP BY
    service_type;
```
**Answer: green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}**

### Question 6. P97/P95/P90 Taxi Monthly Fare
```sql
WITH filtered_trips AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM
        `datawarehousing-de-zoomcamp.dbt_vbarreiros.fct_taxi_trips`
    WHERE
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit Card')
),
percentiles AS (
    SELECT
        service_type,
        year,
        month,
        APPROX_QUANTILES(fare_amount, 100) AS fare_percentiles
    FROM
        filtered_trips
    GROUP BY
        service_type,
        year,
        month
)
SELECT
    service_type,
    year,
    month,
    fare_percentiles[OFFSET(97)] AS p97,
    fare_percentiles[OFFSET(95)] AS p95,
    fare_percentiles[OFFSET(90)] AS p90
FROM
    percentiles
WHERE
    year = 2020
    AND month = 4
ORDER BY
    service_type;
```
**Answer: green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 52.0, p95: 37.0, p90: 25.5} **

### Question 7. Top #Nth longest P90 travel time Location for FHV
```sql
```
**Answer: **