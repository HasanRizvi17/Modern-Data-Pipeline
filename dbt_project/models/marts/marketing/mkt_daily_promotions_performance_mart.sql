WITH

promotions AS (
    SELECT *
    FROM {{ ref('drive_promotions_dim') }}
),

rentals AS (
    SELECT
        rental_id,
        DATE(start_time) AS date,
        start_city,
        end_city,
        is_inter_city_travel,
        package_name,
        promo_id,
        rental_cost
    FROM {{ ref('drive_rentals_fct') }}
    WHERE promo_id IS NOT NULL
),

rental_promotions AS (
    SELECT
        r.promo_id,
        p.promo_code,
        p.discount_type,
        r.date,
        r.start_city,
        r.end_city,
        r.is_inter_city_travel,
        r.package_name,
        -- redemptions
        COUNT(DISTINCT r.rental_id) AS rentals,
        SUM(r.rental_cost) AS rental_revenue,
        SUM({{ apply_discount('p.discount_type', 'p.discount_amount', 'r.rental_cost') }}) AS discount_amount
    FROM promotions AS p
    LEFT JOIN rentals AS r ON r.promo_id = p.promo_id
    GROUP BY 
        r.promo_id, p.promo_code, p.discount_type, 
        r.date, r.start_city, r.end_city, r.is_inter_city_travel, r.package_name
),

roi_calculations AS (
    SELECT
        rp.*,
        ROUND(SAFE_DIVIDE(rental_revenue - discount_amount, discount_amount), 3) AS roi
    FROM rental_promotions AS rp
)

SELECT *
FROM roi_calculations
