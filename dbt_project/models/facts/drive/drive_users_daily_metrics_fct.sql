WITH 

rentals AS (
    SELECT *
    FROM {{ ref('drive_rentals_fct') }}
)

SELECT
    -- grain
    user_id,
    user_city,
    user_country,
    user_market,
    user_tenure_days, 
    user_tenure_days_group,
    end_date AS date,
    
    -- rental metrics
    COUNT(DISTINCT rental_id) AS rentals_count,
    ROUND(SUM(net_revenue)/COUNT(DISTINCT rental_id), 2) AS avg_revenue_per_rental,

    -- payments / financials
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_revenue) AS net_revenue,
    SUM(discount_amount) AS discount_amount,
    SUM(refunded_amount) AS refunded_amount,
    COUNT(DISTINCT CASE WHEN has_refund = TRUE THEN rental_id ELSE NULL END) AS rentals_with_refund,

    -- vehicle usage
    SUM(rental_duration_min) AS total_rental_duration_min,
    SUM(rental_duration_hour) AS total_rental_duration_hour,
    SUM(distance_km) AS total_distance_km,

    -- user experience
    AVG(avg_rating_value) AS avg_rating_value,
    SUM(rating_count) AS rating_count,
    COUNT(DISTINCT CASE WHEN is_5_star_rating = TRUE THEN rental_id ELSE NULL END) AS five_star_rentals,
    COUNT(DISTINCT CASE WHEN is_1_star_rating = TRUE THEN rental_id ELSE NULL END) AS one_star_rentals,

    -- support tickets
    SUM(ticket_count) AS ticket_count,
    COUNT(DISTINCT CASE WHEN has_support_ticket = TRUE THEN rental_id ELSE NULL END) AS rentals_with_ticket,
    COUNT(DISTINCT CASE WHEN ticket_has_escalation = TRUE THEN rental_id ELSE NULL END) AS rentals_with_ticket_escalation,

    -- incidents
    SUM(incident_count) AS incident_count,
    SUM(critical_incident_count) AS critical_incident_count,
    COUNT(DISTINCT CASE WHEN has_incident = TRUE THEN rental_id ELSE NULL END) AS rentals_with_incident

FROM rentals
GROUP BY user_id, user_city, user_country, user_market, user_tenure_days, user_tenure_days_group, date