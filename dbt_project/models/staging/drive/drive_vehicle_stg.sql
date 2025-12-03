WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_vehicle_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS vehicle_id,
        JSON_EXTRACT_SCALAR(data, '$.city_id') AS city_id,
        JSON_EXTRACT_SCALAR(data, '$.model_id') AS model_id,
        JSON_EXTRACT_SCALAR(data, '$.fleet_id') AS fleet_id,
        JSON_EXTRACT_SCALAR(data, '$.status') AS status,
        JSON_EXTRACT_SCALAR(data, '$.battery_level') AS battery_level,
        JSON_EXTRACT_SCALAR(data, '$.fuel_level') AS fuel_level,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        JSON_EXTRACT_SCALAR(data, '$.updated_at') AS updated_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(vehicle_id AS STRING) AS vehicle_id,
        SAFE_CAST(city_id AS STRING) AS city_id,
        SAFE_CAST(model_id AS STRING) AS model_id,
        SAFE_CAST(fleet_id AS STRING) AS fleet_id,
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(battery_level AS INT64) AS battery_level,
        SAFE_CAST(fuel_level AS INT64) AS fuel_level,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated_at)), "Europe/Berlin") AS updated_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        vehicle_id,
        city_id,
        model_id,
        fleet_id,
        LOWER(TRIM(status)) AS status,
        COALESCE(battery_level, 0) AS battery_level,
        COALESCE(fuel_level, 0) AS fuel_level,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization