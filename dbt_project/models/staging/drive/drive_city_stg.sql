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
        SAFE_CAST(city_id AS STRING) AS city_id,
        SAFE_CAST(country_id AS STRING) AS country_id,
        SAFE_CAST(city_name AS STRING) AS city_name,
        SAFE_CAST(timezone AS STRING) AS timezone,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        city_id,
        country_id,
        {{ standardize_string('city_name', lower='no') }} AS city_name,
        {{ standardize_string('timezone', lower='no') }} AS timezone,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization