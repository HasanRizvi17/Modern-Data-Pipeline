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
        {{ dbt.safe_cast('model_id', dbt.type_string()) }} AS model_id,
        {{ dbt.safe_cast('model_name', dbt.type_string()) }} AS model_name,
        {{ dbt.safe_cast('brand', dbt.type_string()) }} AS brand,
        {{ dbt.safe_cast('energy_type', dbt.type_string()) }} AS energy_type,
        {{ dbt.safe_cast('segment', dbt.type_string()) }} AS segment,
        {{ dbt.safe_cast('seats', dbt.type_bigint()) }} AS seats,
        {{ cast_iso_datetimes(['created_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        model_id,
        -- attributes
        {{ standardize_string('model_name') }} AS model_name,
        {{ standardize_string('brand') }} AS brand,
        {{ standardize_string('energy_type') }} AS energy_type,
        {{ standardize_string('segment') }} AS segment,
        COALESCE(seats, 0) AS seats,
        -- timestamps
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization