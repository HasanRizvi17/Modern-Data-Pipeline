WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_payment_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS payment_id,
        JSON_EXTRACT_SCALAR(data, '$.rental_id') AS rental_id,
        JSON_EXTRACT_SCALAR(data, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(data, '$.amount') AS amount,
        JSON_EXTRACT_SCALAR(data, '$.status') AS status,
        JSON_EXTRACT_SCALAR(data, '$.method') AS method,
        JSON_EXTRACT_SCALAR(data, '$.currency') AS currency,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
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
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        payment_id,
        rental_id,
        user_id,
        COALESCE(amount, 0) AS amount,
        LOWER(TRIM(status)) AS status,
        LOWER(TRIM(method)) AS method,
        currency,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization