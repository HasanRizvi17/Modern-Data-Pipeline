WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_promotion_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS promo_id,
        JSON_EXTRACT_SCALAR(data, '$.code') AS promo_code,
        JSON_EXTRACT_SCALAR(data, '$.discount_amount') AS discount_amount,
        JSON_EXTRACT_SCALAR(data, '$.discount_type') AS discount_type,
        JSON_EXTRACT_SCALAR(data, '$.start_date') AS start_date,
        JSON_EXTRACT_SCALAR(data, '$.end_date') AS end_date,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(promo_id AS STRING) AS promo_id,
        SAFE_CAST(promo_code AS STRING) AS promo_code,
        SAFE_CAST(discount_amount AS FLOAT64) AS discount_amount,
        SAFE_CAST(discount_type AS STRING) AS discount_type,
        SAFE_CAST(start_date AS DATE) AS start_date,
        SAFE_CAST(end_date AS DATE) AS end_date,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        promo_id,
        LOWER(TRIM(promo_code)) AS promo_code,
        discount_amount,
        LOWER(TRIM(discount_type)) AS discount_type,
        start_date,
        end_date,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization