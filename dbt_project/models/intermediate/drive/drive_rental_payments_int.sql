WITH

payments AS (
    SELECT *
    FROM {{ ref('drive_payment_stg') }}
),

refunds AS (
    SELECT *
    FROM {{ ref('drive_refund_stg') }}
),

promotions AS (
    SELECT *
    FROM {{ ref('drive_promotion_stg') }}
),

rentals AS (
    SELECT *
    FROM {{ ref('drive_rental_base_int') }}
),

fx_rates AS (
    SELECT *
    FROM {{ ref('ext_fx_rates_int') }}
),

-- payments converted to EUR per payment row, so multi-currency rentals
-- (e.g. a failed USD attempt followed by a successful EUR payment) sum
-- correctly into a single rental_id-grain row below
payments_eur AS (
    SELECT
        p.rental_id,
        SUM(CASE WHEN p.status = 'succeeded' THEN {{ convert_to_euro('p.amount', 'fx.rate') }} ELSE 0 END) AS paid_amount,
        SUM(CASE WHEN p.status = 'failed' THEN {{ convert_to_euro('p.amount', 'fx.rate') }} ELSE 0 END) AS failed_amount,
        SUM(CASE WHEN p.status = 'succeeded' AND p.method = 'wallet' THEN {{ convert_to_euro('p.amount', 'fx.rate') }} ELSE 0 END) AS wallet_paid_amount,
        SUM(CASE WHEN p.status = 'succeeded' AND p.method = 'card' THEN {{ convert_to_euro('p.amount', 'fx.rate') }} ELSE 0 END) AS card_paid_amount
    FROM payments AS p
    LEFT JOIN rentals AS r ON p.rental_id = r.rental_id
    LEFT JOIN fx_rates AS fx ON DATE(r.end_time) = fx.date AND p.currency = fx.from_currency
    GROUP BY p.rental_id
),

-- true refunded amount from drive_refund_stg (captures partial refunds that
-- leave the payment status as 'succeeded'), not just status = 'refunded'
refunds_eur AS (
    SELECT
        p.rental_id,
        SUM({{ convert_to_euro('rf.amount', 'fx.rate') }}) AS refunded_amount
    FROM refunds AS rf
    INNER JOIN payments AS p ON rf.payment_id = p.payment_id
    LEFT JOIN rentals AS r ON p.rental_id = r.rental_id
    LEFT JOIN fx_rates AS fx ON DATE(r.end_time) = fx.date AND p.currency = fx.from_currency
    GROUP BY p.rental_id
),

promotions_applied AS (
    SELECT
        r.rental_id,
        {{ convert_to_euro(apply_discount('pr.discount_type', 'pr.discount_amount', 'r.rental_cost'), 'fx.rate') }} AS discount_amount
    FROM rentals AS r
    INNER JOIN promotions AS pr ON r.promo_id = pr.promo_id
    LEFT JOIN fx_rates AS fx ON DATE(r.end_time) = fx.date AND r.currency = fx.from_currency
    WHERE r.promo_id IS NOT NULL
),

rental_financials AS (
    SELECT
        COALESCE(p.rental_id, pr.rental_id, rf.rental_id) AS rental_id,
        ROUND(COALESCE(p.paid_amount, 0), 2) AS paid_amount,
        ROUND(COALESCE(rf.refunded_amount, 0), 2) AS refunded_amount,
        ROUND(COALESCE(p.failed_amount, 0), 2) AS failed_amount,
        ROUND(COALESCE(p.wallet_paid_amount, 0), 2) AS wallet_paid_amount,
        ROUND(COALESCE(p.card_paid_amount, 0), 2) AS card_paid_amount,
        ROUND(COALESCE(pr.discount_amount, 0), 2) AS discount_amount,
        ROUND(COALESCE(p.paid_amount, 0) + COALESCE(pr.discount_amount, 0), 2) AS gross_revenue,
        ROUND(COALESCE(p.paid_amount, 0) - COALESCE(rf.refunded_amount, 0), 2) AS net_revenue,
        CASE
            WHEN COALESCE(rf.refunded_amount, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS has_refund,
        CASE
            WHEN COALESCE(pr.discount_amount, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS has_promotion
    FROM payments_eur AS p
    FULL JOIN promotions_applied AS pr ON p.rental_id = pr.rental_id
    FULL JOIN refunds_eur AS rf ON COALESCE(p.rental_id, pr.rental_id) = rf.rental_id
)

SELECT *
FROM rental_financials
