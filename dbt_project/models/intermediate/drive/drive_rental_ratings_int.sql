WITH

rental_ratings AS (
    SELECT *
    FROM {{ ref('drive_rental_rating_stg') }}
),

rentals AS (
    SELECT *
    FROM {{ ref('drive_rental_stg') }}
),

ratings_data AS (
    SELECT
        rr.rental_id,
        rr.score,
        rr.comment,
        rr.created_at,
        r.end_time AS rental_end_at,
        TIMESTAMP_DIFF(rr.created_at, r.end_time, HOUR) AS rating_delay_hours
    FROM rental_ratings AS rr
    LEFT JOIN rentals AS r ON rr.rental_id = r.rental_id
),

ratings_aggregated AS (
    SELECT
        rental_id AS rental_id,
        AVG(score) AS avg_rating_value,
        COUNT(*) AS rating_count,
        MIN(rating_delay_hours) AS first_rating_delay_hours,
        CASE WHEN AVG(score) = 5 THEN TRUE ELSE FALSE END AS is_5_star_rating,
        CASE WHEN AVG(score) = 1 THEN TRUE ELSE FALSE END AS is_1_star_rating
    FROM ratings_data
    GROUP BY rental_id
)

SELECT *
FROM ratings_aggregated