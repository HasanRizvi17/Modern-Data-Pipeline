WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_rental_raw') }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'rental_id', 'path': '$.id'},
            {'name': 'user_id', 'path': '$.user_id'},
            {'name': 'vehicle_id', 'path': '$.vehicle_id'},
            {'name': 'package_id', 'path': '$.package_id'},
            {'name': 'start_city_id', 'path': '$.start_city_id'},
            {'name': 'end_city_id', 'path': '$.end_city_id'},
            {'name': 'start_time', 'path': '$.start_time'},
            {'name': 'end_time', 'path': '$.end_time'},
            {'name': 'status', 'path': '$.status'},
            {'name': 'distance_km', 'path': '$.distance_km'},
            {'name': 'rental_cost', 'path': '$.final_cost'},
            {'name': 'promo_id', 'path': '$.promo_id'},
            {'name': 'reservation_id', 'path': '$.reservation_id'},
            {'name': 'incident_id', 'path': '$.incident_id'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
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
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(distance_km AS FLOAT64) AS distance_km,
        SAFE_CAST(rental_cost AS FLOAT64) AS rental_cost,
        SAFE_CAST(promo_id AS STRING) AS promo_id,
        SAFE_CAST(reservation_id AS STRING) AS reservation_id,
        SAFE_CAST(incident_id AS STRING) AS incident_id,
        {{ cast_iso_datetimes(['start_time', 'end_time', 'created_at', 'updated_at']) }},
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
        {{ standardize_string('status') }} AS status,
        COALESCE(distance_km, 0) AS distance_km,
        COALESCE(rental_cost, 0) AS rental_cost,
        promo_id,
        reservation_id,
        incident_id,
        created_at,
        DATE(created_at) AS created_date,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization