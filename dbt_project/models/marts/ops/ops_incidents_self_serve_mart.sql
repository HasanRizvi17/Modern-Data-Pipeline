{{ config(materialized='view') }}

WITH

rentals AS (
    SELECT *
    FROM {{ ref('drive_rentals_fct') }}
),

incidents AS (
    SELECT *
    FROM {{ ref('drive_incidents_fct') }}
),

incidents_360 AS (
    SELECT
        -- incidents data
        i.* EXCEPT(created_at, created_date, updated_at, ingestion_timestamp),
        -- related rentals data
        -- IDs (for clustering tables)
        r.model_id,
        r.fleet_id,
        r.package_id,
        r.start_city_id,
        r.end_city_id,
        r.reservation_id,
        r.promo_id,
        -- rental times
        r.start_time,
        r.end_time,
        -- rental dimensions
        r.start_city,
        r.end_city,
        r.is_inter_city_travel,
        r.rental_status,
        r.package_name,
        r.model_name,
        r.brand,
        r.energy_type,
        r.vehicle_segment,
        r.vehicle_seats,
        r.fleet_name,
        r.company_type,
        -- durations and distance
        r.rental_duration_min,
        r.rental_duration_hour,
        r.distance_km,
        -- payments / financials
        r.rental_cost,
        r.paid_amount,
        r.refunded_amount,
        r.failed_amount,
        r.wallet_paid_amount,
        r.card_paid_amount,
        r.discount_amount,
        r.gross_revenue,
        r.net_revenue,
        r.has_refund,
        r.used_promotion,
        -- ratings
        r.avg_rating_value,
        r.rating_count,
        r.is_5_star_rating,
        r.is_1_star_rating,
        -- ticket timestamps
        i.created_at AS incident_created_at,
        i.updated_at AS incident_updated_at,
    FROM incidents AS i
    LEFT JOIN rentals AS r ON r.rental_id = i.rental_id
)

SELECT *
FROM incidents_360

