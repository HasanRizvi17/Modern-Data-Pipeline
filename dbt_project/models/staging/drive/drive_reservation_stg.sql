WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_reservation_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS reservation_id,
        JSON_EXTRACT_SCALAR(data, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(data, '$.vehicle_id') AS vehicle_id,
        JSON_EXTRACT_SCALAR(data, '$.city_id') AS city_id,
        JSON_EXTRACT_SCALAR(data, '$.package_id') AS package_id,
        JSON_EXTRACT_SCALAR(data, '$.reserved_at') AS reserved_at,
        JSON_EXTRACT_SCALAR(data, '$.reservation_start_at') AS reservation_start_at,
        JSON_EXTRACT_SCALAR(data, '$.reservation_end_at') AS reservation_end_at,
        JSON_EXTRACT_SCALAR(data, '$.status') AS status,
        JSON_EXTRACT_SCALAR(data, '$.cancellation_reason') AS cancellation_reason,
        JSON_EXTRACT_SCALAR(data, '$.cancelled_at') AS cancelled_at,
        JSON_EXTRACT_SCALAR(data, '$.rental_id') AS rental_id,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        JSON_EXTRACT_SCALAR(data, '$.updated_at') AS updated_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(reservation_id AS STRING) AS reservation_id,
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(vehicle_id AS STRING) AS vehicle_id,
        SAFE_CAST(city_id AS STRING) AS city_id,
        SAFE_CAST(package_id AS STRING) AS package_id,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', reserved_at)), "Europe/Berlin") AS reserved_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', reservation_start_at)), "Europe/Berlin") AS reservation_start_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', reservation_end_at)), "Europe/Berlin") AS reservation_end_at,
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(cancellation_reason AS STRING) AS cancellation_reason,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', cancelled_at)), "Europe/Berlin") AS cancelled_at,
        SAFE_CAST(rental_id AS STRING) AS rental_id,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated_at)), "Europe/Berlin") AS updated_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        reservation_id,
        user_id,
        vehicle_id,
        city_id,
        package_id,
        reserved_at,
        reservation_start_at,
        reservation_end_at,
        LOWER(TRIM(status)) AS status,
        NULLIF(TRIM(cancellation_reason), '') AS cancellation_reason,
        cancelled_at,
        rental_id,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization