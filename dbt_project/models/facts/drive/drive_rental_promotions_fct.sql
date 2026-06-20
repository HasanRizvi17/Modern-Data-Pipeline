WITH

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

promotions_applied AS (
    SELECT
        pr.* EXCEPT(ingestion_timestamp),
        r.rental_id, r.start_time, r.end_time, r.rental_cost,
        r.start_city, r.end_city,
        {{ apply_discount('pr.discount_type', 'pr.discount_amount', 'r.rental_cost') }} AS discount_amount_local,
        fx.rate AS fx_rate
    FROM promotions AS pr
    LEFT JOIN rentals AS r ON r.promo_id = pr.promo_id
    LEFT JOIN fx_rates AS fx ON DATE(r.end_time) = fx.date AND r.currency = fx.from_currency
)

SELECT
    * EXCEPT(discount_amount_local, fx_rate),
    {{ convert_to_euro('discount_amount_local', 'fx_rate') }} AS discount_amount_eur
FROM promotions_applied
