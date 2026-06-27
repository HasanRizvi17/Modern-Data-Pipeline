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
        {{ dbt.safe_cast('vehicle_id', dbt.type_string()) }} AS vehicle_id,
        {{ dbt.safe_cast('city_id', dbt.type_string()) }} AS city_id,
        {{ dbt.safe_cast('model_id', dbt.type_string()) }} AS model_id,
        {{ dbt.safe_cast('fleet_id', dbt.type_string()) }} AS fleet_id,
        {{ dbt.safe_cast('status', dbt.type_string()) }} AS status,
        {{ dbt.safe_cast('battery_level', dbt.type_bigint()) }} AS battery_level,
        {{ dbt.safe_cast('fuel_level', dbt.type_bigint()) }} AS fuel_level,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        vehicle_id,
        city_id,
        model_id,
        fleet_id,
        -- attributes
        {{ standardize_string('status') }} AS status,
        COALESCE(battery_level, 0) AS battery_level,
        COALESCE(fuel_level, 0) AS fuel_level,
        -- timestamps
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization