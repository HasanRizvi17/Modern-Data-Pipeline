WITH 

base AS (
    SELECT
        d.date,
        f.* EXCEPT(date, user_city, user_country, user_market, user_tenure_days_group)
    FROM {{ ref('dates_dim') }} AS d
    LEFT JOIN {{ ref('drive_users_daily_metrics_fct') }} AS f ON d.date = f.date
)

SELECT
    -- grain
    date,

    -- rental metrics
    COUNT(DISTINCT user_id) AS active_users,
    COALESCE(SUM(rentals_count), 0) AS total_rentals,
    SAFE_DIVIDE(COALESCE(SUM(rentals_count), 0), COUNT(DISTINCT user_id)) AS avg_rentals_per_user,
    SAFE_DIVIDE(COALESCE(SUM(net_revenue), 0), COUNT(DISTINCT user_id)) AS avg_revenue_per_user,
    SAFE_DIVIDE(COALESCE(SUM(net_revenue), 0), COALESCE(SUM(rentals_count), 0)) AS avg_revenue_per_rental,

    -- payments / financials
    COALESCE(SUM(gross_revenue), 0) AS gross_revenue,
    COALESCE(SUM(net_revenue), 0) AS net_revenue,
    COALESCE(SUM(discount_amount), 0) AS discount_amount,
    COALESCE(SUM(refunded_amount), 0) AS refunded_amount,
    COALESCE(SUM(rentals_with_refund), 0) AS rentals_with_refund,

    -- vehicle usage
    COALESCE(SUM(total_rental_duration_min), 0) AS total_rental_duration_min,
    COALESCE(SUM(total_rental_duration_hour), 0) AS total_rental_duration_hour,
    COALESCE(SUM(total_distance_km), 0) AS total_distance_km,

    -- user experience
    AVG(avg_rating_value) AS avg_rating_value,
    COALESCE(SUM(five_star_rentals), 0) AS five_star_rentals,
    COALESCE(SUM(one_star_rentals), 0) AS one_star_rentals,

    -- support
    COALESCE(SUM(ticket_count), 0) AS ticket_count,
    COALESCE(SUM(rentals_with_ticket), 0) AS rentals_with_ticket,
    COALESCE(SUM(rentals_with_ticket_escalation), 0) AS rentals_with_ticket_escalation,

    -- incidents
    COALESCE(SUM(incident_count), 0) AS incident_count,
    COALESCE(SUM(critical_incident_count), 0) AS critical_incident_count,
    COALESCE(SUM(rentals_with_incident), 0) AS rentals_with_incident

FROM base
GROUP BY date
ORDER BY date
