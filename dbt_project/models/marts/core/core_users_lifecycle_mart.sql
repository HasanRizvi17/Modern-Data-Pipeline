WITH

users AS (
    SELECT
        user_id,
        city_name AS user_city,
        country AS user_country,
        market AS user_market,
        user_tenure_days,
        user_tenure_days_group
    FROM {{ ref('drive_users_dim') }}
),

users_daily_metrics AS (
    SELECT *
    FROM {{ ref('drive_users_daily_metrics_fct') }}
),

-- RFM (Recency, Frequency, Monetary) inputs, lifetime to date
rfm_inputs AS (
    SELECT
        u.user_id,
        u.user_city,
        u.user_country,
        u.user_market,
        u.user_tenure_days,
        u.user_tenure_days_group,
        DATE_DIFF(CURRENT_DATE(), MAX(r.date), DAY) AS recency_days,
        COALESCE(SUM(r.rentals_count), 0) AS frequency_total_rentals,
        COALESCE(SUM(r.net_revenue), 0) AS monetary_lifetime_net_revenue
    FROM users AS u
    LEFT JOIN users_daily_metrics AS r ON u.user_id = r.user_id
    GROUP BY u.user_id, u.user_city, u.user_country, u.user_market, u.user_tenure_days, u.user_tenure_days_group
)

SELECT
    -- grain
    user_id,
    user_city,
    user_country,
    user_market,
    user_tenure_days,
    user_tenure_days_group,

    -- RFM inputs
    recency_days,
    frequency_total_rentals,
    monetary_lifetime_net_revenue,

    -- lifecycle segmentation:
    -- new: signed up within the last 30 days, regardless of activity
    -- churned: never rented, or no activity in 90+ days
    -- at_risk: no activity in 31-90 days (declining engagement, not yet churned)
    -- active: activity within the last 30 days
    CASE
        WHEN user_tenure_days <= 30 THEN 'new'
        WHEN frequency_total_rentals = 0 OR recency_days IS NULL THEN 'churned'
        WHEN recency_days <= 30 THEN 'active'
        WHEN recency_days <= 90 THEN 'at_risk'
        ELSE 'churned'
    END AS lifecycle_stage,

    CURRENT_TIMESTAMP() AS loaded_at

FROM rfm_inputs
