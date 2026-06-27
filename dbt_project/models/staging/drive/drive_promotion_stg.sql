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
        {{ dbt.safe_cast('promo_id', dbt.type_string()) }} AS promo_id,
        {{ dbt.safe_cast('promo_code', dbt.type_string()) }} AS promo_code,
        {{ dbt.safe_cast('discount_amount', dbt.type_float()) }} AS discount_amount,
        {{ dbt.safe_cast('discount_type', dbt.type_string()) }} AS discount_type,
        {{ dbt.safe_cast('start_date', 'date') }} AS start_date,
        {{ dbt.safe_cast('end_date', 'date') }} AS end_date,
        {{ cast_iso_datetimes(['created_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        promo_id,
        -- attributes
        {{ standardize_string('promo_code') }} AS promo_code,
        discount_amount,
        {{ standardize_string('discount_type') }} AS discount_type,
        start_date,
        end_date,
        -- timestamps
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization