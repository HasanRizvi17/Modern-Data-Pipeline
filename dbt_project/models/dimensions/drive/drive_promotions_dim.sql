{{ config(materialized='view') }}

WITH

promotions AS (
    SELECT *
    FROM {{ ref('drive_promotion_stg') }}
)

SELECT *
FROM promotions

