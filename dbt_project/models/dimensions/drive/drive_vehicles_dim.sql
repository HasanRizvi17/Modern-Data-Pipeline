{{ config(materialized='view') }}

WITH

vehicles AS (
    SELECT *
    FROM {{ ref('drive_vehicles_int') }}
)

SELECT *
FROM vehicles

