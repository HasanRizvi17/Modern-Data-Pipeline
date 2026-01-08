{{ config(materialized='view') }}

WITH 

incidents AS (
    SELECT *
    FROM {{ ref('drive_incident_stg') }}
)

SELECT
    incident_id,
    rental_id,
    vehicle_id,
    user_id AS customer_id,
    incident_type,
    severity,
    description,
    police_report_filed,
    estimated_cost,
    reported_at,
    resolved_at,
    TIMESTAMP_DIFF(resolved_at, reported_at, HOUR) AS resolution_time_hours,
    created_at,
    DATE(created_at) AS created_date,
    updated_at
FROM incidents
