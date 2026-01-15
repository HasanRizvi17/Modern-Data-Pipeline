WITH  

calendar AS (
    SELECT date
    FROM {{ ref('dates_dim') }}
),

fx_rates AS (
    SELECT *
    FROM {{ ref('ext_fx_rates_stg') }}
),

fx_rates_transformed AS (
    SELECT
        date,
        to_currency AS from_currency,
        'EUR' AS to_currency,
        1 / rate AS rate
    FROM fx_rates
),

-- building full date × currency spine
date_currency_spine AS (
    SELECT
        c.date,
        f.from_currency,
        f.to_currency
    FROM calendar AS c
    CROSS JOIN (
        SELECT DISTINCT
            from_currency,
            to_currency
        FROM fx_rates_transformed
    ) AS f
),

joined AS (
    SELECT
        s.date,
        s.from_currency,
        s.to_currency,
        r.rate
    FROM date_currency_spine AS s
    LEFT JOIN fx_rates_transformed AS r
        ON  s.date = r.date
        AND s.from_currency = r.from_currency
        AND s.to_currency = r.to_currency
),

forward_filled AS (
    SELECT
        date,
        from_currency,
        to_currency,
        LAST_VALUE(rate IGNORE NULLS) OVER (PARTITION BY from_currency, to_currency ORDER BY date) AS rate
    FROM joined
)

SELECT *
FROM forward_filled
