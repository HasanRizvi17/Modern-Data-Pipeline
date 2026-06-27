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
        {{ dbt.safe_cast('country_id', dbt.type_string()) }} AS country_id,
        {{ dbt.safe_cast('country_name', dbt.type_string()) }} AS country_name,
        {{ dbt.safe_cast('iso_code', dbt.type_string()) }} AS iso_code,
        {{ dbt.safe_cast('currency', dbt.type_string()) }} AS currency,
        {{ dbt.safe_cast('market_id', dbt.type_string()) }} AS market_id,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        country_id,
        market_id,
        -- attributes
        country_name,
        iso_code,
        currency,
        -- timestamps
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization