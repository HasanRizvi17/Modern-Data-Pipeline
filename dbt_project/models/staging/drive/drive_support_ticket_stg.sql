{% set raw_source = source('drive_raw', 'drive_support_ticket_raw') %}

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
            {'name': 'ticket_id', 'path': '$.id'},
            {'name': 'user_id', 'path': '$.user_id'},
            {'name': 'rental_id', 'path': '$.rental_id'},
            {'name': 'incident_id', 'path': '$.incident_id'},
            {'name': 'vehicle_id', 'path': '$.vehicle_id'},
            {'name': 'category', 'path': '$.category'},
            {'name': 'subject', 'path': '$.subject'},
            {'name': 'description', 'path': '$.description'},
            {'name': 'priority', 'path': '$.priority'},
            {'name': 'status', 'path': '$.status'},
            {'name': 'channel', 'path': '$.channel'},
            {'name': 'satisfaction_rating', 'path': '$.satisfaction_rating'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        {{ dbt.safe_cast('ticket_id', dbt.type_string()) }} AS ticket_id,
        {{ dbt.safe_cast('user_id', dbt.type_string()) }} AS user_id,
        {{ dbt.safe_cast('rental_id', dbt.type_string()) }} AS rental_id,
        {{ dbt.safe_cast('incident_id', dbt.type_string()) }} AS incident_id,
        {{ dbt.safe_cast('vehicle_id', dbt.type_string()) }} AS vehicle_id,
        {{ dbt.safe_cast('category', dbt.type_string()) }} AS category,
        {{ dbt.safe_cast('subject', dbt.type_string()) }} AS subject,
        {{ dbt.safe_cast('description', dbt.type_string()) }} AS description,
        {{ dbt.safe_cast('priority', dbt.type_string()) }} AS priority,
        {{ dbt.safe_cast('status', dbt.type_string()) }} AS status,
        {{ dbt.safe_cast('channel', dbt.type_string()) }} AS channel,
        {{ dbt.safe_cast('satisfaction_rating', dbt.type_bigint()) }} AS satisfaction_rating,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        ticket_id,
        user_id,
        rental_id,
        incident_id,
        vehicle_id,
        -- attributes
        {{ standardize_string('category') }} AS category,
        {{ standardize_string('subject') }} AS subject,
        {{ standardize_string('description') }} AS description,
        {{ standardize_string('priority') }} AS priority,
        {{ standardize_string('status') }} AS status,
        {{ standardize_string('channel') }} AS channel,
        COALESCE(satisfaction_rating, 0) AS satisfaction_rating,
        -- timestamps
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization