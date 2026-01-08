{{ config(materialized='table') }}

WITH 

date_spine AS (
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "(SELECT MIN(DATE(created_at)) FROM " ~ ref('drive_rental_stg') ~ ")",
        end_date   = "(SELECT MAX(DATE(created_at)) FROM " ~ ref('drive_rental_stg') ~ ")"
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
    DATE(DATE_TRUNC(date_day, WEEK(MONDAY))) AS week_start_date,
    DATE(DATE_TRUNC(date_day, MONTH)) AS month_start_date,
    DATE(DATE_TRUNC(date_day, QUARTER)) AS quarter_start_date,
    DATE(DATE_TRUNC(date_day, YEAR)) AS year_start_date,

    -- labels and flags
    FORMAT_DATE('%A', date_day) AS day_name,
    FORMAT_DATE('%B', date_day) AS month_name,
    EXTRACT(DAYOFWEEK FROM date_day) IN (1,7) AS is_weekend

FROM date_spine
ORDER BY date
