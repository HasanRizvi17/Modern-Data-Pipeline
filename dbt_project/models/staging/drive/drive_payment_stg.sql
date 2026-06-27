{% set raw_source = source('drive_raw', 'drive_payment_raw') %}

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
        {{ dbt.safe_cast('payment_id', dbt.type_string()) }} AS payment_id,
        {{ dbt.safe_cast('rental_id', dbt.type_string()) }} AS rental_id,
        {{ dbt.safe_cast('user_id', dbt.type_string()) }} AS user_id,
        {{ dbt.safe_cast('amount', dbt.type_float()) }} AS amount,
        {{ dbt.safe_cast('status', dbt.type_string()) }} AS status,
        {{ dbt.safe_cast('method', dbt.type_string()) }} AS method,
        {{ dbt.safe_cast('currency', dbt.type_string()) }} AS currency,
        {{ cast_iso_datetimes(['created_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        payment_id,
        rental_id,
        user_id,
        -- attributes
        COALESCE(amount, 0) AS amount,
        {{ standardize_string('status') }} AS status,
        {{ standardize_string('method') }} AS method,
        currency,
        -- timestamps
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization