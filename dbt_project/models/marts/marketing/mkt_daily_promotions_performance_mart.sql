WITH

promotions AS (
    SELECT *
    FROM {{ ref('drive_promotions_dim') }}
),

dates AS (
    SELECT date
    FROM {{ ref('dates_dim') }}
),

-- promo x date spine, bounded to each promo's active window (which may be
-- forward-dated), so zero-redemption days appear instead of being silently
-- absent and breaking the mart's own not_null test on date
promo_dates AS (
    SELECT
        p.promo_id,
        p.promo_code,
        p.discount_type,
        d.date
    FROM promotions AS p
    CROSS JOIN dates AS d
    WHERE d.date >= p.start_date
      AND d.date <= COALESCE(p.end_date, CURRENT_DATE())
),

metrics AS (
    SELECT *
    FROM {{ ref('drive_promotions_daily_metrics_fct') }}
),

daily AS (
    SELECT
        pd.promo_id,
        pd.promo_code,
        pd.discount_type,
        pd.date,
        COALESCE(m.rentals_count, 0) AS rentals,
        COALESCE(m.rental_revenue, 0) AS rental_revenue,
        COALESCE(m.discount_amount, 0) AS discount_amount,
        COALESCE(m.redemptions, 0) AS redemptions,
        COALESCE(m.unique_users_redeemed, 0) AS unique_users_redeemed,
        COALESCE(m.repeat_redemptions, 0) AS repeat_redemptions
    FROM promo_dates AS pd
    LEFT JOIN metrics AS m ON pd.promo_id = m.promo_id AND pd.date = m.date
)

SELECT
    -- promo dimensions
    promo_id,
    promo_code,
    discount_type,
    -- grain
    date,
    -- redemptions
    rentals,
    rental_revenue,
    discount_amount,
    -- redemption-rate / repeat-usage
    redemptions,
    unique_users_redeemed,
    repeat_redemptions,
    -- discount payback (not a true ROI: no ad-spend or counterfactual baseline)
    ROUND(SAFE_DIVIDE(rental_revenue - discount_amount, discount_amount), 3) AS discount_payback_ratio,
    -- rolling 7d trend: ratio of rolled-up sums, not an average of daily ratios
    ROUND(SAFE_DIVIDE(
        SUM(rental_revenue) OVER (rolling_7d) - SUM(discount_amount) OVER (rolling_7d),
        SUM(discount_amount) OVER (rolling_7d)
    ), 3) AS rolling_7d_discount_payback_ratio,
    CURRENT_TIMESTAMP() AS loaded_at
FROM daily
WINDOW rolling_7d AS (PARTITION BY promo_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
