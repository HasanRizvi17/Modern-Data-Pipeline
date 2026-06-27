{% set raw_source = source('drive_raw', 'drive_user_promotion_raw') %}

WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ raw_source }}
    {{ limit_data_in_dev('timestamp', raw_source) }}
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
        {{ dbt.safe_cast('user_id', dbt.type_string()) }} AS user_id,
        {{ dbt.safe_cast('promo_id', dbt.type_string()) }} AS promo_id,
        {{ cast_iso_datetimes(['redeemed_at', 'created_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        user_id,
        promo_id,
        -- timestamps
        redeemed_at,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization
