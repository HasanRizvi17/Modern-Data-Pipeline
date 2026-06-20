{{ config(materialized='view') }}

WITH

rentals AS (
    SELECT *
    FROM {{ ref('drive_rentals_fct') }}
),

tickets AS (
    SELECT *
    FROM {{ ref('drive_support_tickets_fct') }}
),

tickets_360 AS (
    SELECT
        -- tickets data
        t.*,
        -- related rentals data
        -- IDs (for clustering tables)
        r.vehicle_id,
        r.model_id,
        r.fleet_id,
        r.package_id,
        r.start_city_id,
        r.end_city_id,
        r.reservation_id,
        r.incident_id,
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
        r.rental_cost_eur AS rental_cost,
        r.paid_amount_eur AS paid_amount,
        r.refunded_amount_eur AS refunded_amount,
        r.failed_amount_eur AS failed_amount,
        r.wallet_paid_amount_eur AS wallet_paid_amount,
        r.card_paid_amount_eur AS card_paid_amount,
        r.discount_amount_eur AS discount_amount,
        r.gross_revenue_eur AS gross_revenue,
        r.net_revenue_eur AS net_revenue,
        r.has_refund,
        r.has_promotion,
        -- ratings
        r.avg_rating_value,
        r.rating_count,
        r.is_5_star_rating,
        r.is_1_star_rating

    FROM tickets AS t
    LEFT JOIN rentals AS r ON r.rental_id = t.rental_id
)

SELECT *
FROM tickets_360

