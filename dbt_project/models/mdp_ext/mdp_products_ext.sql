SELECT *
FROM {{ source('mdp_raw', 'mdp_products_raw') }}
