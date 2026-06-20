WITH

rentals AS (
    SELECT
        rental_id,
        promo_id,
        DATE(start_time) AS date,
        rental_cost_eur AS rental_cost,
        discount_amount_eur AS discount_amount
    FROM {{ ref('drive_rentals_fct') }}
    WHERE promo_id IS NOT NULL
      AND rental_status != 'cancelled'
),

-- only actual redemptions: drive_user_promotion_stg also contains promotions
-- assigned to a user but not yet redeemed (redeemed_at IS NULL), which aren't
-- a redemption event and would otherwise distort both the date grain and the
-- repeat-redemption ranking below
redemptions AS (
    SELECT *
    FROM {{ ref('drive_user_promotion_stg') }}
    WHERE redeemed_at IS NOT NULL
),

redemptions_ranked AS (
    SELECT
        promo_id,
        user_id,
        redeemed_at,
        ROW_NUMBER() OVER (PARTITION BY user_id, promo_id ORDER BY redeemed_at) AS redemption_rank
    FROM redemptions
),

rental_metrics AS (
    SELECT
        promo_id,
        date,
        COUNT(DISTINCT rental_id) AS rentals_count,
        SUM(rental_cost) AS rental_revenue,
        SUM(discount_amount) AS discount_amount
    FROM rentals
    GROUP BY promo_id, date
),

-- redemption-rate / repeat-usage metrics from drive_user_promotion_stg
redemption_metrics AS (
    SELECT
        promo_id,
        DATE(redeemed_at) AS date,
        COUNT(*) AS redemptions,
        COUNT(DISTINCT user_id) AS unique_users_redeemed,
        COUNTIF(redemption_rank > 1) AS repeat_redemptions
    FROM redemptions_ranked
    GROUP BY promo_id, date
)

SELECT
    COALESCE(rm.promo_id, rd.promo_id) AS promo_id,
    COALESCE(rm.date, rd.date) AS date,
    COALESCE(rm.rentals_count, 0) AS rentals_count,
    COALESCE(rm.rental_revenue, 0) AS rental_revenue,
    COALESCE(rm.discount_amount, 0) AS discount_amount,
    COALESCE(rd.redemptions, 0) AS redemptions,
    COALESCE(rd.unique_users_redeemed, 0) AS unique_users_redeemed,
    COALESCE(rd.repeat_redemptions, 0) AS repeat_redemptions
FROM rental_metrics AS rm
FULL JOIN redemption_metrics AS rd ON rm.promo_id = rd.promo_id AND rm.date = rd.date
