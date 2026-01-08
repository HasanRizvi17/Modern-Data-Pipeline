{{ config(materialized='view') }}

WITH

users AS (
    SELECT *
    FROM {{ ref('drive_users_int') }}
)

SELECT *
FROM users


