{{
    config(
        materialized='table'
    )
}}

WITH quarterly_revenue AS (
    SELECT
        service_type,
        year,
        quarter,
        year_quarter,
        SUM(total_amount) AS quarterly_revenue
    FROM
        {{ ref('fct_taxi_trips') }}
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
            ((curr.quarterly_revenue - prev.quarterly_revenue) / prev.quarterly_revenue) * 100,
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
)
SELECT
    service_type,
    year_quarter,
    current_quarter_revenue,
    previous_quarter_revenue,
    yoy_growth_percent
FROM
    yoy_growth
ORDER BY
    service_type,
    year_quarter