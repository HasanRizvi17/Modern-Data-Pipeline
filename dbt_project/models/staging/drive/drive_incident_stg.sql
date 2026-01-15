WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_incident_raw') }}
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
        SAFE_CAST(incident_id AS STRING) AS incident_id,
        SAFE_CAST(rental_id AS STRING) AS rental_id,
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(vehicle_id AS STRING) AS vehicle_id,
        SAFE_CAST(city_id AS STRING) AS city_id,
        SAFE_CAST(incident_type AS STRING) AS incident_type,
        SAFE_CAST(severity AS STRING) AS severity,
        SAFE_CAST(description AS STRING) AS description,
        SAFE_CAST(estimated_cost AS FLOAT64) AS estimated_cost,
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(police_report_filed AS BOOL) AS police_report_filed,
        {{ cast_iso_datetimes(['reported_at', 'incident_time', 'resolved_at', 'created_at', 'updated_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        incident_id,
        rental_id,
        user_id,
        vehicle_id,
        city_id,
        reported_at,
        incident_time,
        {{ standardize_string('status') }} AS status,
        {{ standardize_string('incident_type') }} AS incident_type,
        {{ standardize_string('severity') }} AS severity,
        {{ standardize_string('description') }} AS description,
        COALESCE(estimated_cost, 0) AS estimated_cost,
        resolved_at,
        police_report_filed,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization