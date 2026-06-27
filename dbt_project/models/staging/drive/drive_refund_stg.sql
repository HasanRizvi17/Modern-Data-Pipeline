{% set raw_source = source('drive_raw', 'drive_refund_raw') %}

WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ raw_source }}
    {{ limit_data_in_dev('timestamp', raw_source) }}
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
        {{ dbt.safe_cast('refund_id', dbt.type_string()) }} AS refund_id,
        {{ dbt.safe_cast('payment_id', dbt.type_string()) }} AS payment_id,
        {{ dbt.safe_cast('amount', dbt.type_float()) }} AS amount,
        {{ dbt.safe_cast('reason', dbt.type_string()) }} AS reason,
        {{ cast_iso_datetimes(['created_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        refund_id,
        payment_id,
        -- attributes
        COALESCE(amount, 0) AS amount,
        {{ standardize_string('reason') }} AS reason,
        -- timestamps
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization