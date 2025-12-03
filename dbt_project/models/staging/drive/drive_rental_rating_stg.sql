WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_rental_rating_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS rating_id,
        JSON_EXTRACT_SCALAR(data, '$.rental_id') AS rental_id,
        JSON_EXTRACT_SCALAR(data, '$.score') AS score,
        JSON_EXTRACT_SCALAR(data, '$.comment') AS comment,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(rating_id AS STRING) AS rating_id,
        SAFE_CAST(rental_id AS STRING) AS rental_id,
        SAFE_CAST(score AS INT64) AS score,
        SAFE_CAST(comment AS STRING) AS comment,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        rating_id,
        rental_id,
        COALESCE(score, 0) AS score,
        NULLIF(TRIM(comment), '') AS comment,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization