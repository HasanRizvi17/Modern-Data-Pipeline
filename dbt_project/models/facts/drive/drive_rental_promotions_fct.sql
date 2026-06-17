WITH

promotions AS (
    SELECT *
    FROM {{ ref('drive_promotion_stg') }}
),

rentals AS (
    SELECT *
    FROM {{ ref('drive_rental_stg') }}
),

cities AS (
    SELECT *
    FROM {{ ref('drive_city_stg') }}
),

countries AS (
    SELECT *
    FROM {{ ref('drive_country_stg') }}
),

fx_rates AS (
    SELECT *
    FROM {{ ref('ext_fx_rates_int') }}
),

promotions_applied AS (
    SELECT
        pr.* EXCEPT(ingestion_timestamp),
        r.rental_id, r.start_time, r.end_time, r.rental_cost,
        sc.city_name AS start_city, ec.city_name AS end_city,
        {{ apply_discount('pr.discount_type', 'pr.discount_amount', 'r.rental_cost') }} AS discount_amount_local,
        fx.rate AS fx_rate
    FROM promotions AS pr
    LEFT JOIN rentals AS r ON r.promo_id = pr.promo_id
    LEFT JOIN cities AS sc ON r.start_city_id = sc.city_id
    LEFT JOIN cities AS ec ON r.end_city_id = ec.city_id
    LEFT JOIN countries AS sct ON sc.country_id = sct.country_id
    LEFT JOIN fx_rates AS fx ON DATE(r.end_time) = fx.date AND sct.currency = fx.from_currency
)

SELECT
    * EXCEPT(discount_amount_local, fx_rate),
    {{ convert_to_euro('discount_amount_local', 'fx_rate') }} AS discount_amount_eur
FROM promotions_applied