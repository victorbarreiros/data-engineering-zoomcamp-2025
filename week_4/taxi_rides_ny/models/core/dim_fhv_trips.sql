{{
    config(
        materialized='table'
    )
}}

WITH filtered_trips AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount
    FROM
        {{ ref('fct_taxi_trips') }}
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
    fare_percentiles[SAFE_OFFSET(97)] AS p97,
    fare_percentiles[SAFE_OFFSET(95)] AS p95,
    fare_percentiles[SAFE_OFFSET(90)] AS p90
FROM
    percentiles
WHERE
    year = 2020
    AND month = 4
ORDER BY
    service_type;