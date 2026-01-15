SELECT
    date,
    from_currency,
    to_currency,
    rate
FROM {{ source('external_data', 'ext_fx_rates_raw') }}
