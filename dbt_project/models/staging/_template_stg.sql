{{ config(enabled=false) }}

-- TEMPLATE — not a real model (enabled=false above), safe to leave in models/staging/.
-- How to use:
--   1. Copy this file, rename to <entity>_stg.sql (e.g. drive_<entity>_stg.sql).
--   2. Fill in <source_name> / <table_name> and the json_extract_fields list below.
--   3. Decide whether to keep or delete the limit_data_in_dev() line:
--        - keep it for fact-like/transactional sources (high volume, append-style)
--        - delete it for dimension/reference sources (low volume, FK parents of fact tables)
--      See planning/Limiting Data in Dev.md for why this distinction matters.
--   4. Add the matching schema.yml entry (column docs + tests) for the new model.

{% set raw_source = source('<source_name>', '<table_name>_raw') %}

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
            {'name': '<field_name>', 'path': '$.<field_path>'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        {{ dbt.safe_cast('<field_name>', dbt.type_string()) }} AS <field_name>,
        {{ cast_iso_datetimes(['created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        <field_name>,
        -- attributes
        {{ standardize_string('<field_name>') }} AS <field_name>,
        -- timestamps
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization
