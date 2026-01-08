WITH

payments AS (
    SELECT *
    FROM {{ ref('drive_payment_stg') }}
),

refunds AS (
    SELECT *
    FROM {{ ref('drive_refund_stg') }}
),

user_promotions AS (
    SELECT *
    FROM {{ ref('drive_user_promotion_stg') }}
),

promotions AS (
    SELECT *
    FROM {{ ref('drive_promotion_stg') }}
),

rentals AS (
    SELECT *
    FROM {{ ref('drive_rental_stg') }}
),

payments_aggregated AS (
    SELECT
        p.rental_id,
        SUM(CASE WHEN p.status = 'succeeded' THEN p.amount ELSE 0 END) AS paid_amount,
        SUM(CASE WHEN p.status = 'refunded' THEN p.amount ELSE 0 END) AS refunded_amount,
        SUM(CASE WHEN p.status = 'failed' THEN p.amount ELSE 0 END) AS failed_amount,
        SUM(CASE WHEN p.status = 'succeeded' AND p.method = 'wallet' THEN p.amount ELSE 0 END) AS wallet_paid_amount,
        SUM(CASE WHEN p.status = 'succeeded' AND p.method = 'card' THEN p.amount ELSE 0 END) AS card_paid_amount
    FROM payments AS p
    GROUP BY p.rental_id
),

promotions_applied AS (
    SELECT
        r.rental_id,
        SUM(
            CASE
                WHEN discount_type = 'fixed' THEN pr.discount_amount 
                WHEN discount_type = 'percentage' THEN pr.discount_amount * rental_cost
            END
        ) AS discount_amount
    FROM user_promotions AS up
    LEFT JOIN promotions AS pr ON up.promo_id = pr.promo_id
    INNER JOIN rentals AS r ON r.promo_id = pr.promo_id
    GROUP BY r.rental_id, r.rental_cost
),

rental_financials AS (
    SELECT
        COALESCE(p.rental_id, pr.rental_id) AS rental_id,
        ROUND(COALESCE(p.paid_amount, 0), 2) AS paid_amount,
        ROUND(COALESCE(p.refunded_amount, 0), 2) AS refunded_amount,
        ROUND(COALESCE(failed_amount, 0), 2) AS failed_amount,
        ROUND(COALESCE(wallet_paid_amount, 0), 2) AS wallet_paid_amount,
        ROUND(COALESCE(card_paid_amount, 0), 2) AS card_paid_amount,
        ROUND(COALESCE(pr.discount_amount, 0), 2) AS discount_amount,
        ROUND(COALESCE(p.paid_amount, 0) + COALESCE(pr.discount_amount, 0), 2) AS gross_revenue,
        ROUND(COALESCE(p.paid_amount, 0) - COALESCE(p.refunded_amount, 0), 2) AS net_revenue,
        CASE
            WHEN COALESCE(p.refunded_amount, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS has_refund,
        CASE
            WHEN COALESCE(pr.discount_amount, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS used_promotion
    FROM payments_aggregated AS p
    FULL JOIN promotions_applied AS pr ON p.rental_id = pr.rental_id
)

SELECT *
FROM rental_financials


