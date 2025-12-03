WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_ticket_history_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS history_id,
        JSON_EXTRACT_SCALAR(data, '$.ticket_id') AS ticket_id,
        JSON_EXTRACT_SCALAR(data, '$.changed_at') AS changed_at,
        JSON_EXTRACT_SCALAR(data, '$.changed_by') AS changed_by,
        JSON_EXTRACT_SCALAR(data, '$.field_changed') AS field_changed,
        JSON_EXTRACT_SCALAR(data, '$.old_value') AS old_value,
        JSON_EXTRACT_SCALAR(data, '$.new_value') AS new_value,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(history_id AS STRING) AS history_id,
        SAFE_CAST(ticket_id AS STRING) AS ticket_id,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', changed_at)), "Europe/Berlin") AS changed_at,
        SAFE_CAST(changed_by AS STRING) AS changed_by,
        SAFE_CAST(field_changed AS STRING) AS field_changed,
        SAFE_CAST(old_value AS STRING) AS old_value,
        SAFE_CAST(new_value AS STRING) AS new_value,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        history_id,
        ticket_id,
        changed_at,
        LOWER(TRIM(changed_by)) AS changed_by,
        LOWER(TRIM(field_changed)) AS field_changed,
        NULLIF(TRIM(old_value), '') AS old_value,
        NULLIF(TRIM(new_value), '') AS new_value,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization
