{% set raw_source = source('drive_raw', 'drive_incident_raw') %}

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
            {'name': 'incident_id', 'path': '$.id'},
            {'name': 'rental_id', 'path': '$.rental_id'},
            {'name': 'user_id', 'path': '$.user_id'},
            {'name': 'vehicle_id', 'path': '$.vehicle_id'},
            {'name': 'city_id', 'path': '$.city_id'},
            {'name': 'reported_at', 'path': '$.reported_at'},
            {'name': 'incident_time', 'path': '$.incident_time'},
            {'name': 'incident_type', 'path': '$.type'},
            {'name': 'severity', 'path': '$.severity'},
            {'name': 'description', 'path': '$.description'},
            {'name': 'estimated_cost', 'path': '$.estimated_cost'},
            {'name': 'status', 'path': '$.status'},
            {'name': 'resolved_at', 'path': '$.resolved_at'},
            {'name': 'police_report_filed', 'path': '$.police_report_filed'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        {{ dbt.safe_cast('incident_id', dbt.type_string()) }} AS incident_id,
        {{ dbt.safe_cast('rental_id', dbt.type_string()) }} AS rental_id,
        {{ dbt.safe_cast('user_id', dbt.type_string()) }} AS user_id,
        {{ dbt.safe_cast('vehicle_id', dbt.type_string()) }} AS vehicle_id,
        {{ dbt.safe_cast('city_id', dbt.type_string()) }} AS city_id,
        {{ dbt.safe_cast('incident_type', dbt.type_string()) }} AS incident_type,
        {{ dbt.safe_cast('severity', dbt.type_string()) }} AS severity,
        {{ dbt.safe_cast('description', dbt.type_string()) }} AS description,
        {{ dbt.safe_cast('estimated_cost', dbt.type_float()) }} AS estimated_cost,
        {{ dbt.safe_cast('status', dbt.type_string()) }} AS status,
        {{ dbt.safe_cast('police_report_filed', 'boolean') }} AS police_report_filed,
        {{ cast_iso_datetimes(['reported_at', 'incident_time', 'resolved_at', 'created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        incident_id,
        rental_id,
        user_id,
        vehicle_id,
        city_id,
        -- attributes
        {{ standardize_string('status') }} AS status,
        {{ standardize_string('incident_type') }} AS incident_type,
        {{ standardize_string('severity') }} AS severity,
        {{ standardize_string('description') }} AS description,
        COALESCE(estimated_cost, 0) AS estimated_cost,
        police_report_filed,
        -- timestamps
        reported_at,
        incident_time,
        resolved_at,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization