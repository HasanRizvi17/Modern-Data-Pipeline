SELECT *
FROM {{ source('mdp_raw', 'mdp_sellers_raw') }}
