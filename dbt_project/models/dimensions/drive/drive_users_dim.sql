{{ config(materialized='view') }}

WITH

users AS (
    SELECT *
    FROM {{ ref('drive_users_int') }}
)

-- Type-1 dimension: direct passthrough of drive_users_int, no historization
SELECT *
FROM users


