WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_vehicle_fleet_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'fleet_id', 'path': '$.id'},
            {'name': 'fleet_name', 'path': '$.name'},
            {'name': 'company_type', 'path': '$.company_type'},
            {'name': 'city_id', 'path': '$.city_id'},
            {'name': 'created_at', 'path': '$.created_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(fleet_id AS STRING) AS fleet_id,
        SAFE_CAST(fleet_name AS STRING) AS fleet_name,
        SAFE_CAST(company_type AS STRING) AS company_type,
        SAFE_CAST(city_id AS STRING) AS city_id,
        {{ cast_iso_datetimes(['created_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        fleet_id,
        {{ standardize_string('fleet_name', lower='no') }} AS fleet_name,
        {{ standardize_string('company_type') }} AS company_type,
        city_id,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization