SELECT *
FROM {{ source('mdp_raw', 'mdp_order_items_raw') }}
