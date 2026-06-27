WITH 

users AS (
    SELECT
        user_id,
        city_name,
        country,
        market,
        user_tenure_days,
        user_tenure_days_group
    FROM {{ ref('drive_users_dim') }}
),

users_daily_metrics AS (
    SELECT *
    FROM {{ ref('drive_users_daily_metrics_fct') }}
),

-- rolling 30d activity recency, joined separately so it doesn't affect the
-- lifetime aggregates above
rolling_30d AS (
    SELECT
        user_id,
        COUNT(DISTINCT date) AS rolling_30d_active_days,
        COALESCE(SUM(rentals_count), 0) AS rolling_30d_rentals,
        COALESCE(SUM(net_revenue), 0) AS rolling_30d_net_revenue
    FROM users_daily_metrics
    WHERE date >= {{ dbt.dateadd('day', -29, 'CURRENT_DATE()') }}
    GROUP BY user_id
)

{% set week_trunc = dbt.date_trunc('week', 'date') %}
{% set month_trunc = dbt.date_trunc('month', 'date') %}

SELECT
    -- grain
    u.user_id,
    u.city_name AS user_city,
    u.country AS user_country,
    u.market AS user_market, 
    u.user_tenure_days,
    u.user_tenure_days_group,

    -- activity
    COALESCE(COUNT(DISTINCT {{ dbt.date_trunc('day', 'date') }}), 0) AS active_days,
    COALESCE(COUNT(DISTINCT {{ week_trunc }}), 0) AS active_weeks,
    COALESCE(COUNT(DISTINCT {{ month_trunc }}), 0) AS active_months,
    COALESCE(COUNT(DISTINCT {{ dbt.date_trunc('quarter', 'date') }}), 0) AS active_quarters,
    COALESCE(COUNT(DISTINCT {{ dbt.date_trunc('year', 'date') }}), 0) AS active_years,
    -- activity_rate_days: percentage of days active since between first and last rental date
    ROUND(SAFE_DIVIDE(
        COUNT(DISTINCT {{ dbt.date_trunc('day', 'date') }}),
        {{ dbt.datediff('MIN(date)', 'MAX(date)', 'day') }} + 1
    ), 4) AS activity_rate_days,
    -- activity_rate_weeks: percentage of days active since between first and last rental date
    ROUND(SAFE_DIVIDE(
        COUNT(DISTINCT {{ week_trunc }}),
        {{ dbt.datediff('MIN(' ~ week_trunc ~ ')', 'MAX(' ~ week_trunc ~ ')', 'week') }} + 1
    ), 4) AS activity_rate_weeks,
    -- activity_rate_months: percentage of days active since between first and last rental date
    ROUND(SAFE_DIVIDE(
        COUNT(DISTINCT {{ month_trunc }}),
        {{ dbt.datediff('MIN(' ~ month_trunc ~ ')', 'MAX(' ~ month_trunc ~ ')', 'month') }} + 1
    ), 4) AS activity_rate_months,
    MIN(date) AS first_rental_date,
    MAX(date) AS last_rental_date,
    {{ dbt.datediff('MAX(date)', 'CURRENT_DATE()', 'day') }} AS recency_days,

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
    COALESCE(SUM(rentals_with_incident), 0) AS rentals_with_incident,

    -- rolling metrics
    COALESCE(ANY_VALUE(r30.rolling_30d_active_days), 0) AS rolling_30d_active_days,
    COALESCE(ANY_VALUE(r30.rolling_30d_rentals), 0) AS rolling_30d_rentals,
    COALESCE(ANY_VALUE(r30.rolling_30d_net_revenue), 0) AS rolling_30d_net_revenue,

    CURRENT_TIMESTAMP() AS loaded_at

FROM users AS u
LEFT JOIN users_daily_metrics AS r ON u.user_id = r.user_id
LEFT JOIN rolling_30d AS r30 ON u.user_id = r30.user_id
GROUP BY u.user_id, user_city, user_country, user_market, u.user_tenure_days, u.user_tenure_days_group
