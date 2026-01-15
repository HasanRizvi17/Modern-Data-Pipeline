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
            {'name': 'user_id', 'path': '$.user_id'},
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
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(email AS STRING) AS email,
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(city_id AS STRING) AS city_id,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        user_id,
        {{ standardize_string('email') }} AS email,
        {{ standardize_string('status') }} AS status,
        {{ standardize_string('status', lower='no') }} AS status_2,
        city_id,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization