{{ config(materialized='view') }}

WITH

rentals AS (
    SELECT *
    FROM {{ ref('drive_rentals_fct') }}
)

SELECT *
FROM rentals