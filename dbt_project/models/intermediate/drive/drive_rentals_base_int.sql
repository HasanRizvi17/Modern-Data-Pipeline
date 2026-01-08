WITH

rentals AS (
    SELECT *
    FROM {{ ref('drive_rental_stg') }}
),

reservations AS (
    SELECT *
    FROM {{ ref('drive_reservation_stg') }}
),

trip_packages AS (
    SELECT *
    FROM {{ ref('drive_trip_package_stg') }}
),

rentals_base AS (
    SELECT
        r.rental_id,
        r.user_id,
        r.start_city_id,
        r.end_city_id,
        r.vehicle_id AS vehicle_id,
        r.package_id AS package_id,
        r.start_time,
        r.end_time,
        TIMESTAMP_DIFF(r.end_time, r.start_time, MINUTE) AS rental_duration_min,
        TIMESTAMP_DIFF(r.end_time, r.start_time, MINUTE) / 60.0 AS rental_duration_hour,
        r.reservation_id,
        r.incident_id,
        r.promo_id,
        r.status AS rental_status,
        p.package_name,
        CASE 
          WHEN r.start_city_id = r.end_city_id THEN TRUE
          ELSE FALSE
        END AS is_inter_city_travel,
        res.reserved_at,
        res.reservation_start_at,
        res.reservation_end_at,
        res.status,
        res.cancellation_reason,
        res.cancelled_at,
        r.distance_km,
        r.rental_cost,
        r.created_at,
        DATE(r.created_at) AS created_date,
        r.updated_at,
        r.ingestion_timestamp

    FROM rentals AS r
    LEFT JOIN reservations AS res ON r.reservation_id = res.reservation_id
    LEFT JOIN trip_packages AS p ON r.package_id = p.package_id
)

SELECT *
FROM rentals_base
