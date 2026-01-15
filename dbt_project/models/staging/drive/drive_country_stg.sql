WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_country_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'country_id', 'path': '$.id'},
            {'name': 'country_name', 'path': '$.name'},
            {'name': 'iso_code', 'path': '$.iso_code'},
            {'name': 'currency', 'path': '$.currency'},
            {'name': 'market_id', 'path': '$.market_id'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(country_id AS STRING) AS country_id,
        SAFE_CAST(country_name AS STRING) AS country_name,
        SAFE_CAST(iso_code AS STRING) AS iso_code,
        SAFE_CAST(currency AS STRING) AS currency,
        SAFE_CAST(market_id AS STRING) AS market_id,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        country_id,
        country_name,
        iso_code,
        currency,
        market_id,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization