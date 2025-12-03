WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_city_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS city_id,
        JSON_EXTRACT_SCALAR(data, '$.name') AS city_name,
        JSON_EXTRACT_SCALAR(data, '$.country_id') AS country_id,
        JSON_EXTRACT_SCALAR(data, '$.timezone') AS timezone,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        JSON_EXTRACT_SCALAR(data, '$.updated_at') AS updated_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(city_id AS STRING) AS city_id,
        SAFE_CAST(city_name AS STRING) AS city_name,
        SAFE_CAST(country_id AS STRING) AS country_id,
        SAFE_CAST(timezone AS STRING) AS timezone,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated_at)), "Europe/Berlin") AS updated_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        city_id,
        NULLIF(TRIM(city_name), '') AS city_name,
        country_id,
        NULLIF(TRIM(timezone), '') AS timezone,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization