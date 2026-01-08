WITH

rentals AS (
    SELECT *
    FROM {{ ref('drive_rentals_base_int') }}
),

ratings AS (
    SELECT *
    FROM {{ ref('drive_rental_ratings_int') }}
),

vehicles AS (
    SELECT *
    FROM {{ ref('drive_vehicles_int') }}
),

payments AS (
    SELECT *
    FROM {{ ref('drive_rental_payments_int') }}
),

issues AS (
    SELECT *
    FROM {{ ref('drive_rental_issues_int') }}
),

cities AS (
    SELECT *
    FROM {{ ref('drive_city_stg') }}
),

users AS (
    SELECT *
    FROM {{ ref('drive_users_int') }}
)

SELECT
    -- primary key
    r.rental_id,

    -- foreign keys
    r.user_id,
    r.vehicle_id,
    v.model_id,
    v.fleet_id,
    r.package_id,
    r.start_city_id,
    r.end_city_id,
    vc.city_id AS vehicle_city_id,
    r.reservation_id,
    r.incident_id,
    r.promo_id,
    u.city_id AS user_city_id,
    u.country_id AS user_country_id,
    u.market_id AS user_market_id,
    -- user details
    u.user_status,
    u.city AS user_city,
    u.country AS user_country,
    u.market AS user_market, 
    u.user_tenure_days, 
    u.user_tenure_days_group, 
    u.is_active_user, 
    u.is_validated_user,
    -- rental times
    r.start_time,
    r.end_time,
    DATE(r.start_time) AS start_date,
    DATE(r.end_time) AS end_date,
    r.reserved_at,
    r.reservation_start_at,
    r.reservation_end_at,

    -- dimensions
    sc.city_name AS start_city,
    ec.city_name AS end_city,
    vc.city_name AS vehicle_city,
    r.is_inter_city_travel,
    r.rental_status,
    r.package_name,
    v.model_name,
    v.brand,
    v.energy_type,
    v.vehicle_segment,
    v.vehicle_seats,
    v.fleet_name,
    v.company_type,
    
    -- durations and distance
    r.rental_duration_min,
    r.rental_duration_hour,
    r.distance_km,

    -- payments / financials
    r.rental_cost,
    f.paid_amount,
    f.refunded_amount,
    f.failed_amount,
    f.wallet_paid_amount,
    f.card_paid_amount,
    f.discount_amount,
    f.gross_revenue,
    f.net_revenue,
    f.has_refund,
    f.used_promotion,

    -- ratings
    rt.avg_rating_value,
    rt.rating_count,
    rt.first_rating_delay_hours,
    rt.is_5_star_rating,
    rt.is_1_star_rating,

    -- support tickets
    i.ticket_support_rating,
    i.ticket_count,
    i.has_support_ticket,
    i.ticket_has_escalation,
    i.ticket_first_response_at,
    i.ticket_first_escalated_at,
    i.ticket_resolved_at,

    -- incidents
    i.incident_count,
    i.critical_incident_count,
    i.incidents_with_police_report_filed,
    i.incident_estimated_cost,
    i.has_incident,

    -- timestamps
    r.created_at,
    r.created_date,
    r.updated_at,
    r.ingestion_timestamp

FROM rentals AS r
LEFT JOIN vehicles AS v ON r.vehicle_id = v.vehicle_id
LEFT JOIN payments AS f ON r.rental_id = f.rental_id
LEFT JOIN ratings AS rt ON r.rental_id = rt.rental_id
LEFT JOIN issues AS i ON r.rental_id = i.rental_id
LEFT JOIN cities AS sc ON r.start_city_id = sc.city_id -- start city
LEFT JOIN cities AS ec ON r.end_city_id = ec.city_id -- end city
LEFT JOIN cities AS vc ON v.city_id = vc.city_id -- vehicle city
LEFT JOIN users AS u ON r.user_id = u.user_id
