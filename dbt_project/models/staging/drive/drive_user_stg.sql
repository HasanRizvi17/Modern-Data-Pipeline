WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_user_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'user_id', 'path': '$.id'},
            {'name': 'email', 'path': '$.email'},
            {'name': 'status', 'path': '$.status'},
            {'name': 'city_id', 'path': '$.city_id'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        {{ dbt.safe_cast('user_id', dbt.type_string()) }} AS user_id,
        {{ dbt.safe_cast('email', dbt.type_string()) }} AS email,
        {{ dbt.safe_cast('status', dbt.type_string()) }} AS status,
        {{ dbt.safe_cast('city_id', dbt.type_string()) }} AS city_id,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        user_id,
        city_id,
        -- attributes
        {{ standardize_string('email') }} AS email,
        {{ standardize_string('status') }} AS status,
        -- timestamps
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization