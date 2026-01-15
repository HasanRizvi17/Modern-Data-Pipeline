WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_support_ticket_raw') }}
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
        SAFE_CAST(ticket_id AS STRING) AS ticket_id,
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(rental_id AS STRING) AS rental_id,
        SAFE_CAST(incident_id AS STRING) AS incident_id,
        SAFE_CAST(vehicle_id AS STRING) AS vehicle_id,
        SAFE_CAST(category AS STRING) AS category,
        SAFE_CAST(subject AS STRING) AS subject,
        SAFE_CAST(description AS STRING) AS description,
        SAFE_CAST(priority AS STRING) AS priority,
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(channel AS STRING) AS channel,
        SAFE_CAST(satisfaction_rating AS INT64) AS satisfaction_rating,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        ticket_id,
        user_id,
        rental_id,
        incident_id,
        vehicle_id,
        {{ standardize_string('category') }} AS category,
        {{ standardize_string('subject') }} AS subject,
        {{ standardize_string('description') }} AS description,
        {{ standardize_string('priority') }} AS priority,
        {{ standardize_string('status') }} AS status,
        {{ standardize_string('channel') }} AS channel,
        created_at,
        updated_at,
        COALESCE(satisfaction_rating, 0) AS satisfaction_rating,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization