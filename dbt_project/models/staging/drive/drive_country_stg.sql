WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_country_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS country_id,
        JSON_EXTRACT_SCALAR(data, '$.name') AS country_name,
        JSON_EXTRACT_SCALAR(data, '$.iso_code') AS iso_code,
        JSON_EXTRACT_SCALAR(data, '$.currency') AS currency,
        JSON_EXTRACT_SCALAR(data, '$.market_id') AS market_id,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        JSON_EXTRACT_SCALAR(data, '$.updated_at') AS updated_at,
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
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated_at)), "Europe/Berlin") AS updated_at,
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