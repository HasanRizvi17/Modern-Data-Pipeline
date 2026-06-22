WITH

vehicles AS (
    SELECT *
    FROM {{ ref('drive_vehicles_int') }}
)

-- Type-1 dimension: direct passthrough of drive_vehicles_int, no historization
SELECT *
FROM vehicles

