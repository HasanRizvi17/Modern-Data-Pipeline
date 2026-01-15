WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_promotion_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'promo_id', 'path': '$.id'},
            {'name': 'promo_code', 'path': '$.code'},
            {'name': 'discount_amount', 'path': '$.discount_amount'},
            {'name': 'discount_type', 'path': '$.discount_type'},
            {'name': 'start_date', 'path': '$.start_date'},
            {'name': 'end_date', 'path': '$.end_date'},
            {'name': 'created_at', 'path': '$.created_at'}
        ]) }},
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
        {{ cast_iso_datetimes(['created_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        promo_id,
        {{ standardize_string('promo_code') }} AS promo_code,
        discount_amount,
        {{ standardize_string('discount_type') }} AS discount_type,
        start_date,
        end_date,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization