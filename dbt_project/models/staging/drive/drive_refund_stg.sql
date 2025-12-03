WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_refund_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS refund_id,
        JSON_EXTRACT_SCALAR(data, '$.payment_id') AS payment_id,
        JSON_EXTRACT_SCALAR(data, '$.amount') AS amount,
        JSON_EXTRACT_SCALAR(data, '$.reason') AS reason,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(refund_id AS STRING) AS refund_id,
        SAFE_CAST(payment_id AS STRING) AS payment_id,
        SAFE_CAST(amount AS FLOAT64) AS amount,
        SAFE_CAST(reason AS STRING) AS reason,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        refund_id,
        payment_id,
        COALESCE(amount, 0) AS amount,
        NULLIF(TRIM(reason), '') AS reason,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization