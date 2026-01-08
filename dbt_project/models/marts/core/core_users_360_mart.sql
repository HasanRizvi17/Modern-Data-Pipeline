WITH 

users AS (
    SELECT
        user_id,
        city,
        country,
        market,
        user_tenure_days,
        user_tenure_days_group
    FROM {{ ref('drive_users_dim') }}
),

users_daily_metrics AS (
    SELECT *
    FROM {{ ref('drive_users_daily_metrics_fct') }}
)

SELECT
    -- grain
    u.user_id,
    u.city AS user_city,
    u.country AS user_country,
    u.market AS user_market, 
    u.user_tenure_days,
    u.user_tenure_days_group,

    -- activity
    COALESCE(COUNT(DISTINCT DATE_TRUNC(date, DAY)), 0) AS active_days,
    COALESCE(COUNT(DISTINCT DATE_TRUNC(date, WEEK)), 0) AS active_weeks,
    COALESCE(COUNT(DISTINCT DATE_TRUNC(date, MONTH)), 0) AS active_months,
    COALESCE(COUNT(DISTINCT DATE_TRUNC(date, QUARTER)), 0) AS active_quarters,
    COALESCE(COUNT(DISTINCT DATE_TRUNC(date, YEAR)), 0) AS active_years,
    -- activity_rate_days: percentage of days active since between first and last rental date
    ROUND(SAFE_DIVIDE(
        COUNT(DISTINCT DATE_TRUNC(date, DAY)),
        DATE_DIFF(MAX(date), MIN(date), DAY) + 1
    ), 4) AS activity_rate_days,
    -- activity_rate_weeks: percentage of days active since between first and last rental date
    ROUND(SAFE_DIVIDE(
        COUNT(DISTINCT DATE_TRUNC(date, WEEK)),
        DATE_DIFF(MAX(DATE_TRUNC(date, WEEK)), MIN(DATE_TRUNC(date, WEEK)), WEEK) + 1
    ), 4) AS activity_rate_weeks,
    -- activity_rate_months: percentage of days active since between first and last rental date
    ROUND(SAFE_DIVIDE(
        COUNT(DISTINCT DATE_TRUNC(date, MONTH)),
        DATE_DIFF(MAX(DATE_TRUNC(date, MONTH)), MIN(DATE_TRUNC(date, MONTH)), MONTH) + 1
    ), 4) AS activity_rate_months,
    MIN(date) AS first_rental_date,
    MAX(date) AS last_rental_date,
    DATE_DIFF(CURRENT_DATE(), MAX(date), DAY) AS recency_days,

    -- rental metrics
    COALESCE(SUM(rentals_count), 0) AS total_rentals,
    ROUND(SAFE_DIVIDE(SUM(net_revenue), SUM(rentals_count)), 2) AS avg_revenue_per_rental,

    -- payments / financials
    COALESCE(SUM(gross_revenue), 0) AS lifetime_gross_revenue,
    COALESCE(SUM(net_revenue), 0) AS lifetime_net_revenue,
    COALESCE(SUM(discount_amount), 0) AS lifetime_discount_amount,
    COALESCE(SUM(refunded_amount), 0) AS lifetime_refunded_amount,
    COALESCE(SUM(rentals_with_refund), 0) AS rentals_with_refund,

    -- vehicle usage
    COALESCE(SUM(total_rental_duration_min), 0) AS total_rental_duration_min,
    COALESCE(SUM(total_rental_duration_hour), 0) AS total_rental_duration_hour,
    COALESCE(SUM(total_distance_km), 0) AS total_distance_km,
    SAFE_DIVIDE(
        SUM(total_rental_duration_hour),
        NULLIF(SUM(rentals_count), 0)
    ) AS avg_rental_duration_hour,

    -- user experience
    COALESCE(SUM(rating_count), 0) AS rating_count,
    SUM(rating_count * avg_rating_value) / NULLIF(SUM(rating_count), 0) AS avg_rating_value,
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


FROM users AS u
LEFT JOIN users_daily_metrics AS r ON u.user_id = r.user_id
GROUP BY u.user_id, user_city, user_country, user_market, u.user_tenure_days, u.user_tenure_days_group
