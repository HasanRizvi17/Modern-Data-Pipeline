SELECT *
FROM {{ source('mdp_raw', 'mdp_customers_raw') }}
