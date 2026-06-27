WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_city_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'city_id', 'path': '$.id'},
            {'name': 'country_id', 'path': '$.country_id'},
            {'name': 'city_name', 'path': '$.name'},
            {'name': 'timezone', 'path': '$.timezone'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        {{ dbt.safe_cast('city_id', dbt.type_string()) }} AS city_id,
        {{ dbt.safe_cast('country_id', dbt.type_string()) }} AS country_id,
        {{ dbt.safe_cast('city_name', dbt.type_string()) }} AS city_name,
        {{ dbt.safe_cast('timezone', dbt.type_string()) }} AS timezone,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        city_id,
        country_id,
        -- attributes
        {{ standardize_string('city_name', lower='no') }} AS city_name,
        {{ standardize_string('timezone', lower='no') }} AS timezone,
        -- timestamps
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization