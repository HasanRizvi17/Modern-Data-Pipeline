{% set raw_source = source('drive_raw', 'drive_ticket_history_raw') %}

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
        {{ dbt.safe_cast('history_id', dbt.type_string()) }} AS history_id,
        {{ dbt.safe_cast('ticket_id', dbt.type_string()) }} AS ticket_id,
        {{ dbt.safe_cast('changed_by', dbt.type_string()) }} AS changed_by,
        {{ dbt.safe_cast('field_changed', dbt.type_string()) }} AS field_changed,
        {{ dbt.safe_cast('old_value', dbt.type_string()) }} AS old_value,
        {{ dbt.safe_cast('new_value', dbt.type_string()) }} AS new_value,
        {{ cast_iso_datetimes(['changed_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        history_id,
        ticket_id,
        -- attributes
        {{ standardize_string('changed_by') }} AS changed_by,
        {{ standardize_string('field_changed') }} AS field_changed,
        {{ standardize_string('old_value') }} AS old_value,
        {{ standardize_string('new_value') }} AS new_value,
        -- timestamps
        changed_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization
