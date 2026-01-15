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

cities AS (
    SELECT *
    FROM {{ ref('drive_city_stg') }}
),

countries AS (
    SELECT *
    FROM {{ ref('drive_country_stg') }}
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
        sc.city_name AS start_city,
        ec.city_name AS end_city,
        sct.country_name AS start_country,
        ect.country_name AS end_country,
        CASE 
          WHEN r.start_city_id = r.end_city_id THEN TRUE
          ELSE FALSE
        END AS is_inter_city_travel,
        sct.currency,
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
    LEFT JOIN cities AS sc ON r.start_city_id = sc.city_id -- start city
    LEFT JOIN cities AS ec ON r.end_city_id = ec.city_id -- end city
    LEFT JOIN countries AS sct ON sc.country_id = sct.country_id -- start country
    LEFT JOIN countries AS ect ON ec.country_id = ect.country_id -- end country
)

SELECT *
FROM rentals_base
