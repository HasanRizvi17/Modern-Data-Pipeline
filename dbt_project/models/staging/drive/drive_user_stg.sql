WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_user_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS user_id,
        JSON_EXTRACT_SCALAR(data, '$.email') AS email,
        JSON_EXTRACT_SCALAR(data, '$.status') AS status,
        JSON_EXTRACT_SCALAR(data, '$.city_id') AS city_id,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        JSON_EXTRACT_SCALAR(data, '$.updated_at') AS updated_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(email AS STRING) AS email,
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(city_id AS STRING) AS city_id,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated_at)), "Europe/Berlin") AS updated_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        user_id,
        NULLIF(TRIM(email), '') AS email,
        LOWER(TRIM(status)) AS status,
        city_id,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization