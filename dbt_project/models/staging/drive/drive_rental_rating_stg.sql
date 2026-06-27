{% set raw_source = source('drive_raw', 'drive_rental_rating_raw') %}

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
            {'name': 'rating_id', 'path': '$.id'},
            {'name': 'rental_id', 'path': '$.rental_id'},
            {'name': 'score', 'path': '$.score'},
            {'name': 'comment', 'path': '$.comment'},
            {'name': 'created_at', 'path': '$.created_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        {{ dbt.safe_cast('rating_id', dbt.type_string()) }} AS rating_id,
        {{ dbt.safe_cast('rental_id', dbt.type_string()) }} AS rental_id,
        {{ dbt.safe_cast('score', dbt.type_bigint()) }} AS score,
        {{ dbt.safe_cast('comment', dbt.type_string()) }} AS comment,
        {{ cast_iso_datetimes(['created_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        rating_id,
        rental_id,
        -- attributes
        COALESCE(score, 0) AS score,
        {{ standardize_string('comment') }} AS comment,
        -- timestamps
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization