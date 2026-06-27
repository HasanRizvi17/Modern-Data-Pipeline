WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_market_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'market_id', 'path': '$.id'},
            {'name': 'market_name', 'path': '$.name'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        {{ dbt.safe_cast('market_id', dbt.type_string()) }} AS market_id,
        {{ dbt.safe_cast('market_name', dbt.type_string()) }} AS market_name,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        market_id,
        -- attributes
        {{ standardize_string('market_name') }} AS market_name,
        -- timestamps
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization