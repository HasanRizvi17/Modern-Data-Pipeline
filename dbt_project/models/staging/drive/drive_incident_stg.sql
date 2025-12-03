WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_incident_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS incident_id,
        JSON_EXTRACT_SCALAR(data, '$.rental_id') AS rental_id,
        JSON_EXTRACT_SCALAR(data, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(data, '$.vehicle_id') AS vehicle_id,
        JSON_EXTRACT_SCALAR(data, '$.city_id') AS city_id,
        JSON_EXTRACT_SCALAR(data, '$.reported_at') AS reported_at,
        JSON_EXTRACT_SCALAR(data, '$.incident_time') AS incident_time,
        JSON_EXTRACT_SCALAR(data, '$.type') AS type,
        JSON_EXTRACT_SCALAR(data, '$.severity') AS severity,
        JSON_EXTRACT_SCALAR(data, '$.description') AS description,
        JSON_EXTRACT_SCALAR(data, '$.estimated_cost') AS estimated_cost,
        JSON_EXTRACT_SCALAR(data, '$.status') AS status,
        JSON_EXTRACT_SCALAR(data, '$.resolved_at') AS resolved_at,
        JSON_EXTRACT_SCALAR(data, '$.police_report_filed') AS police_report_filed,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        JSON_EXTRACT_SCALAR(data, '$.updated_at') AS updated_at,
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
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', reported_at)), "Europe/Berlin") AS reported_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', incident_time)), "Europe/Berlin") AS incident_time,
        SAFE_CAST(type AS STRING) AS type,
        SAFE_CAST(severity AS STRING) AS severity,
        SAFE_CAST(description AS STRING) AS description,
        SAFE_CAST(estimated_cost AS FLOAT64) AS estimated_cost,
        SAFE_CAST(status AS STRING) AS status,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', resolved_at)), "Europe/Berlin") AS resolved_at,
        SAFE_CAST(police_report_filed AS BOOL) AS police_report_filed,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated_at)), "Europe/Berlin") AS updated_at,
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
        LOWER(TRIM(type)) AS type,
        LOWER(TRIM(severity)) AS severity,
        NULLIF(TRIM(description), '') AS description,
        COALESCE(estimated_cost, 0) AS estimated_cost,
        LOWER(TRIM(status)) AS status,
        resolved_at,
        police_report_filed,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization