WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_vehicle_model_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'model_id', 'path': '$.id'},
            {'name': 'model_name', 'path': '$.name'},
            {'name': 'brand', 'path': '$.brand'},
            {'name': 'energy_type', 'path': '$.energy_type'},
            {'name': 'segment', 'path': '$.segment'},
            {'name': 'seats', 'path': '$.seats'},
            {'name': 'created_at', 'path': '$.created_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(model_id AS STRING) AS model_id,
        SAFE_CAST(model_name AS STRING) AS model_name,
        SAFE_CAST(brand AS STRING) AS brand,
        SAFE_CAST(energy_type AS STRING) AS energy_type,
        SAFE_CAST(segment AS STRING) AS segment,
        SAFE_CAST(seats AS INT64) AS seats,
        {{ cast_iso_datetimes(['created_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        model_id,
        {{ standardize_string('model_name') }} AS model_name,
        {{ standardize_string('brand') }} AS brand,
        {{ standardize_string('energy_type') }} AS energy_type,
        {{ standardize_string('segment') }} AS segment,
        COALESCE(seats, 0) AS seats,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization