WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_trip_package_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS package_id,
        JSON_EXTRACT_SCALAR(data, '$.name') AS package_name,
        JSON_EXTRACT_SCALAR(data, '$.active_from') AS active_from,
        JSON_EXTRACT_SCALAR(data, '$.active_to') AS active_to,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(package_id AS STRING) AS package_id,
        SAFE_CAST(package_name AS STRING) AS package_name,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', active_from)), "Europe/Berlin") AS active_from,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', active_to)), "Europe/Berlin") AS active_to,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        package_id,
        LOWER(TRIM(package_name)) AS package_name,
        active_from,
        active_to,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization