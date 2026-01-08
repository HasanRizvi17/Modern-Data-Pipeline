WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_user_promotion_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(data, '$.promo_id') AS promo_id,
        JSON_EXTRACT_SCALAR(data, '$.redeemed_at') AS redeemed_at,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(promo_id AS STRING) AS promo_id,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', redeemed_at)), "Europe/Berlin") AS redeemed_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        user_id,
        promo_id,
        redeemed_at,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization
