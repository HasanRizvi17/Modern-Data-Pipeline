WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_refund_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'refund_id', 'path': '$.id'},
            {'name': 'payment_id', 'path': '$.payment_id'},
            {'name': 'amount', 'path': '$.amount'},
            {'name': 'reason', 'path': '$.reason'},
            {'name': 'created_at', 'path': '$.created_at'} 
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(refund_id AS STRING) AS refund_id,
        SAFE_CAST(payment_id AS STRING) AS payment_id,
        SAFE_CAST(amount AS FLOAT64) AS amount,
        SAFE_CAST(reason AS STRING) AS reason,
        {{ cast_iso_datetimes(['created_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        refund_id,
        payment_id,
        COALESCE(amount, 0) AS amount,
        {{ standardize_string('reason') }} AS reason,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization