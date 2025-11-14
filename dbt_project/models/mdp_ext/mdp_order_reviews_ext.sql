SELECT *
FROM {{ source('mdp_raw', 'mdp_order_reviews_raw') }}
