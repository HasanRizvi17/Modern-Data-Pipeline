{{ config(materialized='table') }}

WITH 

date_spine AS (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "DATE('2024-07-01')",
        end_date   = dbt.dateadd('day', 365, 'CURRENT_DATE()')
    ) }}
)

SELECT
    -- primary key
    DATE(date_day) AS date,

    -- calendar fields
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,

    -- ISO calendar
    EXTRACT(ISOWEEK FROM date_day) AS iso_week,
    EXTRACT(ISOYEAR FROM date_day) AS iso_year,

    -- period starts
    cast({{ dbt.date_trunc('week', 'date_day') }} as date) AS week_start_date,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date) AS month_start_date,
    cast({{ dbt.date_trunc('quarter', 'date_day') }} as date) AS quarter_start_date,
    cast({{ dbt.date_trunc('year', 'date_day') }} as date) AS year_start_date,

    -- labels and flags
    FORMAT_DATE('%A', date_day) AS day_name,
    FORMAT_DATE('%B', date_day) AS month_name,
    EXTRACT(DAYOFWEEK FROM date_day) IN (1,7) AS is_weekend

FROM date_spine
ORDER BY date
