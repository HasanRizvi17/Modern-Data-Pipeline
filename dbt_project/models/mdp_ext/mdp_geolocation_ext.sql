SELECT *
FROM {{ source('mdp_raw', 'mdp_geolocation_raw') }}
