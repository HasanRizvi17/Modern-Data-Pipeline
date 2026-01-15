WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_user_promotion_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'user_id', 'path': '$.user_id'},
            {'name': 'promo_id', 'path': '$.promo_id'},
            {'name': 'redeemed_at', 'path': '$.redeemed_at'},
            {'name': 'created_at', 'path': '$.created_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(promo_id AS STRING) AS promo_id,
        {{ cast_iso_datetimes(['redeemed_at', 'created_at']) }},
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
