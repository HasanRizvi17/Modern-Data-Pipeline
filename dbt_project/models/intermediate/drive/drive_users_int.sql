WITH

users AS (
    SELECT *
    FROM {{ ref('drive_user_stg') }}
),

cities AS (
    SELECT *
    FROM {{ ref('drive_city_stg') }}
),

countries AS (
    SELECT *
    FROM {{ ref('drive_country_stg') }}
),

markets AS (
    SELECT *
    FROM {{ ref('drive_market_stg') }}
),

users_data AS (
    SELECT
        -- IDs
        u.user_id,
        u.city_id,
        co.country_id,
        m.market_id,
        -- user core attributes
        u.email AS user_email,
        u.status AS user_status,
        -- user geography
        c.city_name AS city,
        co.country_name AS country,
        co.iso_code,
        m.market_name AS market,
        -- user tenure
        DATE_DIFF(CURRENT_DATE, date(u.created_at), DAY) AS user_tenure_days,
        CASE
            WHEN DATE_DIFF(CURRENT_DATE, date(u.created_at), DAY) < 30 THEN '< 30 days'
            WHEN DATE_DIFF(CURRENT_DATE, date(u.created_at), DAY) BETWEEN 30 AND 90 THEN '30-90 days'
            WHEN DATE_DIFF(CURRENT_DATE, date(u.created_at), DAY) BETWEEN 91 AND 180 THEN '91-180 days'
            WHEN DATE_DIFF(CURRENT_DATE, date(u.created_at), DAY) BETWEEN 181 AND 365 THEN '> 181-365 days'
            WHEN DATE_DIFF(CURRENT_DATE, date(u.created_at), DAY) > 365 THEN '> 365 days'
        END AS user_tenure_days_group,
        -- user status derived fields 
        CASE
            WHEN u.status = 'active' THEN true
            ELSE false
        END AS is_active_user,
        CASE
            WHEN u.status = 'validated' THEN true
            ELSE false
        END AS is_validated_user,
        -- timestamps
        u.created_at,
        DATE(u.created_at) AS created_date,
        u.updated_at,
        u.ingestion_timestamp

    FROM users AS u
    LEFT JOIN cities AS c ON u.city_id = c.city_id
    LEFT JOIN countries AS co ON c.country_id = co.country_id
    LEFT JOIN markets AS m ON co.market_id = m.market_id
)

SELECT *
FROM users_data
