WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_vehicle_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'vehicle_id', 'path': '$.id'},
            {'name': 'city_id', 'path': '$.city_id'},
            {'name': 'model_id', 'path': '$.model_id'},
            {'name': 'fleet_id', 'path': '$.fleet_id'},
            {'name': 'status', 'path': '$.status'},
            {'name': 'battery_level', 'path': '$.battery_level'},
            {'name': 'fuel_level', 'path': '$.fuel_level'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
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
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        vehicle_id,
        city_id,
        model_id,
        fleet_id,
        {{ standardize_string('status') }} AS status,
        COALESCE(battery_level, 0) AS battery_level,
        COALESCE(fuel_level, 0) AS fuel_level,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization