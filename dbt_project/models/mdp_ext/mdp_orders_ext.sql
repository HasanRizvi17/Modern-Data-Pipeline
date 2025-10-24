SELECT *
FROM {{ source('mdp_raw', 'mdp_orders_raw') }}
