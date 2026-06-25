WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_trip_package_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'package_id', 'path': '$.id'},
            {'name': 'package_name', 'path': '$.name'},
            {'name': 'active_from', 'path': '$.active_from'},
            {'name': 'active_to', 'path': '$.active_to'},
            {'name': 'created_at', 'path': '$.created_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(package_id AS STRING) AS package_id,
        SAFE_CAST(package_name AS STRING) AS package_name,
        {{ cast_iso_datetimes(['active_from', 'active_to', 'created_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        package_id,
        -- attributes
        {{ standardize_string('package_name') }} AS package_name,
        -- timestamps
        active_from,
        active_to,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization