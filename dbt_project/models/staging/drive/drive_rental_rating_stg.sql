WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_rental_rating_raw') }}
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
        SAFE_CAST(rating_id AS STRING) AS rating_id,
        SAFE_CAST(rental_id AS STRING) AS rental_id,
        SAFE_CAST(score AS INT64) AS score,
        SAFE_CAST(comment AS STRING) AS comment,
        {{ cast_iso_datetimes(['created_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        rating_id,
        rental_id,
        COALESCE(score, 0) AS score,
        {{ standardize_string('comment') }} AS comment,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization