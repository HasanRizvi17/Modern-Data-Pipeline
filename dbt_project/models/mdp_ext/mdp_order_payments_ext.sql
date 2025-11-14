SELECT *
FROM {{ source('mdp_raw', 'mdp_order_payments_raw') }}
