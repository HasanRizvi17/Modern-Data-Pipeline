WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_support_ticket_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS ticket_id,
        JSON_EXTRACT_SCALAR(data, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(data, '$.rental_id') AS rental_id,
        JSON_EXTRACT_SCALAR(data, '$.incident_id') AS incident_id,
        JSON_EXTRACT_SCALAR(data, '$.vehicle_id') AS vehicle_id,
        JSON_EXTRACT_SCALAR(data, '$.category') AS category,
        JSON_EXTRACT_SCALAR(data, '$.subject') AS subject,
        JSON_EXTRACT_SCALAR(data, '$.description') AS description,
        JSON_EXTRACT_SCALAR(data, '$.priority') AS priority,
        JSON_EXTRACT_SCALAR(data, '$.status') AS status,
        JSON_EXTRACT_SCALAR(data, '$.channel') AS channel,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        JSON_EXTRACT_SCALAR(data, '$.updated_at') AS updated_at,
        JSON_EXTRACT_SCALAR(data, '$.satisfaction_rating') AS satisfaction_rating,
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
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated_at)), "Europe/Berlin") AS updated_at,
        SAFE_CAST(satisfaction_rating AS INT64) AS satisfaction_rating,
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
        LOWER(TRIM(category)) AS category,
        NULLIF(TRIM(subject), '') AS subject,
        NULLIF(TRIM(description), '') AS description,
        LOWER(TRIM(priority)) AS priority,
        LOWER(TRIM(status)) AS status,
        LOWER(TRIM(channel)) AS channel,
        created_at,
        updated_at,
        COALESCE(satisfaction_rating, 0) AS satisfaction_rating,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization