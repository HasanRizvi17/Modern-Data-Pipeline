{{ config(materialized='view') }}

WITH

cities AS (
    SELECT *
    FROM {{ ref('drive_city_stg') }}
),

countries AS (
    SELECT *
    FROM {{ ref('drive_country_stg') }}
),

markets AS (
    SELECT *
    FROM {{ ref('drive_market_stg') }}
),

cities_data AS (
    SELECT
        c.city_id,
        c.city_name AS city,
        co.country_name AS country,
        co.iso_code,
        m.market_name AS market,
        -- timestamps
        c.created_at,
        DATE(c.created_at) AS created_date,
        c.updated_at,
        c.ingestion_timestamp

    FROM cities AS c
    LEFT JOIN countries AS co ON c.country_id = co.country_id
    LEFT JOIN markets AS m ON co.market_id = m.market_id
)

SELECT *
FROM cities_data

