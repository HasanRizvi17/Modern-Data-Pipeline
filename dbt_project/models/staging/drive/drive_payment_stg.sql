WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_payment_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'payment_id', 'path': '$.id'},
            {'name': 'rental_id', 'path': '$.rental_id'},
            {'name': 'user_id', 'path': '$.user_id'},
            {'name': 'amount', 'path': '$.amount'},
            {'name': 'status', 'path': '$.status'},
            {'name': 'method', 'path': '$.method'},
            {'name': 'currency', 'path': '$.currency'},
            {'name': 'created_at', 'path': '$.created_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(payment_id AS STRING) AS payment_id,
        SAFE_CAST(rental_id AS STRING) AS rental_id,
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(amount AS FLOAT64) AS amount,
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(method AS STRING) AS method,
        SAFE_CAST(currency AS STRING) AS currency,
        {{ cast_iso_datetimes(['created_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        payment_id,
        rental_id,
        user_id,
        COALESCE(amount, 0) AS amount,
        {{ standardize_string('status') }} AS status,
        {{ standardize_string('method') }} AS method,
        currency,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization