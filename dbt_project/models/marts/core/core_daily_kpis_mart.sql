WITH

base AS (
    SELECT
        d.date,
        f.* EXCEPT(date, user_city, user_country, user_market, user_tenure_days_group)
    FROM {{ ref('dates_dim') }} AS d
    LEFT JOIN {{ ref('drive_users_daily_metrics_fct') }} AS f ON d.date = f.date
    WHERE d.date <= CURRENT_DATE()
),

users_daily AS (
    SELECT *
    FROM {{ ref('drive_users_daily_metrics_fct') }}
),

daily_kpis AS (
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
)

SELECT
    dk.*,

    -- rolling metrics (revenue is additive, summed via window; active_users is a
    -- distinct count, so it's recomputed over each rolling window directly,
    -- not summed, to avoid double-counting returning users)
    SUM(dk.net_revenue) OVER (ORDER BY dk.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_net_revenue,
    SUM(dk.net_revenue) OVER (ORDER BY dk.date ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS rolling_28d_net_revenue,
    (
        SELECT COUNT(DISTINCT user_id)
        FROM users_daily AS u
        WHERE u.date BETWEEN {{ dbt.dateadd('day', -6, 'dk.date') }} AND dk.date
    ) AS rolling_7d_active_users,
    (
        SELECT COUNT(DISTINCT user_id)
        FROM users_daily AS u
        WHERE u.date BETWEEN {{ dbt.dateadd('day', -27, 'dk.date') }} AND dk.date
    ) AS rolling_28d_active_users,

    CURRENT_TIMESTAMP() AS loaded_at

FROM daily_kpis AS dk
ORDER BY date
