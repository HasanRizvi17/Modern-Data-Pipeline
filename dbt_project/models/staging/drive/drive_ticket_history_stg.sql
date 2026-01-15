WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_ticket_history_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'history_id', 'path': '$.id'},
            {'name': 'ticket_id', 'path': '$.ticket_id'},
            {'name': 'changed_at', 'path': '$.changed_at'},
            {'name': 'changed_by', 'path': '$.changed_by'},
            {'name': 'field_changed', 'path': '$.field_changed'},
            {'name': 'old_value', 'path': '$.old_value'},
            {'name': 'new_value', 'path': '$.new_value'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(history_id AS STRING) AS history_id,
        SAFE_CAST(ticket_id AS STRING) AS ticket_id,
        SAFE_CAST(changed_by AS STRING) AS changed_by,
        SAFE_CAST(field_changed AS STRING) AS field_changed,
        SAFE_CAST(old_value AS STRING) AS old_value,
        SAFE_CAST(new_value AS STRING) AS new_value,
        {{ cast_iso_datetimes(['changed_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        history_id,
        ticket_id,
        changed_at,
        {{ standardize_string('changed_by') }} AS changed_by,
        {{ standardize_string('field_changed') }} AS field_changed, 
        {{ standardize_string('old_value') }} AS old_value,
        {{ standardize_string('new_value') }} AS new_value,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization
