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
        {{ dbt.safe_cast('fleet_id', dbt.type_string()) }} AS fleet_id,
        {{ dbt.safe_cast('fleet_name', dbt.type_string()) }} AS fleet_name,
        {{ dbt.safe_cast('company_type', dbt.type_string()) }} AS company_type,
        {{ dbt.safe_cast('city_id', dbt.type_string()) }} AS city_id,
        {{ cast_iso_datetimes(['created_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        fleet_id,
        city_id,
        -- attributes
        {{ standardize_string('fleet_name', lower='no') }} AS fleet_name,
        {{ standardize_string('company_type') }} AS company_type,
        -- timestamps
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization