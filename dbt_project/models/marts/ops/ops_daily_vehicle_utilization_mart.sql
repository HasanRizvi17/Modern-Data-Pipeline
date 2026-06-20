WITH

vehicles AS (
    SELECT *
    FROM {{ ref('drive_vehicles_dim') }}
),

dates AS (
    SELECT date
    FROM {{ ref('dates_dim') }}
    WHERE date <= CURRENT_DATE()
),

-- vehicle x date spine, bounded to each vehicle's own active window, so
-- zero-activity dates appear instead of being silently absent
vehicle_dates AS (
    SELECT
        v.vehicle_id,
        v.model_id,
        v.fleet_id,
        v.city_name AS vehicle_city,
        v.model_name,
        v.brand,
        v.energy_type,
        v.vehicle_segment,
        v.vehicle_seats,
        v.fleet_name,
        v.company_type,
        d.date
    FROM vehicles AS v
    CROSS JOIN dates AS d
    WHERE d.date >= v.created_date
),

metrics AS (
    SELECT *
    FROM {{ ref('drive_vehicles_daily_metrics_fct') }}
)

SELECT
    -- IDs
    vd.vehicle_id,
    vd.model_id,
    vd.fleet_id,
    -- date
    vd.date,
    -- vehicle dimensions
    vd.vehicle_city,
    vd.model_name,
    vd.brand,
    vd.energy_type,
    vd.vehicle_segment,
    vd.vehicle_seats,
    vd.fleet_name,
    vd.company_type,
    -- utilization metrics
    COALESCE(m.rentals_count, 0) AS rentals_count,
    COALESCE(m.total_rental_duration_min, 0) AS total_rental_duration_min,
    COALESCE(m.total_rental_duration_hour, 0) AS total_rental_duration_hour,
    COALESCE(m.total_distance_km, 0) AS total_distance_km,
    -- revenue metrics
    COALESCE(m.gross_revenue, 0) AS gross_revenue,
    COALESCE(m.net_revenue, 0) AS net_revenue,
    -- incidents and support
    COALESCE(m.incident_count, 0) AS incident_count,
    COALESCE(m.critical_incident_count, 0) AS critical_incident_count,
    COALESCE(m.ticket_count, 0) AS ticket_count,
    CURRENT_TIMESTAMP() AS loaded_at
FROM vehicle_dates AS vd
LEFT JOIN metrics AS m ON vd.vehicle_id = m.vehicle_id AND vd.date = m.date
