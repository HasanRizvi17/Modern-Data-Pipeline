WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_vehicle_fleet_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS fleet_id,
        JSON_EXTRACT_SCALAR(data, '$.name') AS fleet_name,
        JSON_EXTRACT_SCALAR(data, '$.company_type') AS company_type,
        JSON_EXTRACT_SCALAR(data, '$.city_id') AS city_id,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(fleet_id AS STRING) AS fleet_id,
        SAFE_CAST(fleet_name AS STRING) AS fleet_name,
        SAFE_CAST(company_type AS STRING) AS company_type,
        SAFE_CAST(city_id AS STRING) AS city_id,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        fleet_id,
        NULLIF(TRIM(fleet_name), '') AS fleet_name,
        LOWER(TRIM(company_type)) AS company_type,
        city_id,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization