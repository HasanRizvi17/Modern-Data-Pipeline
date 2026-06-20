{{ config(materialized='view') }}

WITH

promotions AS (
    SELECT *
    FROM {{ ref('drive_promotion_stg') }}
)

-- Type-1 dimension: direct passthrough of drive_promotion_stg, no historization
SELECT *
FROM promotions

