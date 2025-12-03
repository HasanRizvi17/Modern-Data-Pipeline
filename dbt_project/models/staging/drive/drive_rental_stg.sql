WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_rental_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS rental_id,
        JSON_EXTRACT_SCALAR(data, '$.user_id') AS user_id,
        JSON_EXTRACT_SCALAR(data, '$.vehicle_id') AS vehicle_id,
        JSON_EXTRACT_SCALAR(data, '$.package_id') AS package_id,
        JSON_EXTRACT_SCALAR(data, '$.start_city_id') AS start_city_id,
        JSON_EXTRACT_SCALAR(data, '$.end_city_id') AS end_city_id,
        JSON_EXTRACT_SCALAR(data, '$.start_time') AS start_time,
        JSON_EXTRACT_SCALAR(data, '$.end_time') AS end_time,
        JSON_EXTRACT_SCALAR(data, '$.status') AS status,
        JSON_EXTRACT_SCALAR(data, '$.distance_km') AS distance_km,
        JSON_EXTRACT_SCALAR(data, '$.final_cost') AS final_cost,
        JSON_EXTRACT_SCALAR(data, '$.promo_id') AS promo_id,
        JSON_EXTRACT_SCALAR(data, '$.reservation_id') AS reservation_id,
        JSON_EXTRACT_SCALAR(data, '$.incident_id') AS incident_id,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        JSON_EXTRACT_SCALAR(data, '$.updated_at') AS updated_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(rental_id AS STRING) AS rental_id,
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(vehicle_id AS STRING) AS vehicle_id,
        SAFE_CAST(package_id AS STRING) AS package_id,
        SAFE_CAST(start_city_id AS STRING) AS start_city_id,
        SAFE_CAST(end_city_id AS STRING) AS end_city_id,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', start_time)), "Europe/Berlin") AS start_time,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', end_time)), "Europe/Berlin") AS end_time,
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(distance_km AS FLOAT64) AS distance_km,
        SAFE_CAST(final_cost AS FLOAT64) AS final_cost,
        SAFE_CAST(promo_id AS STRING) AS promo_id,
        SAFE_CAST(reservation_id AS STRING) AS reservation_id,
        SAFE_CAST(incident_id AS STRING) AS incident_id,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', updated_at)), "Europe/Berlin") AS updated_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        rental_id,
        user_id,
        vehicle_id,
        package_id,
        start_city_id,
        end_city_id,
        start_time,
        end_time,
        LOWER(TRIM(status)) AS status,
        COALESCE(distance_km, 0) AS distance_km,
        COALESCE(final_cost, 0) AS final_cost,
        promo_id,
        reservation_id,
        incident_id,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization