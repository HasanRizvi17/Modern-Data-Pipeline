WITH 

rentals AS (

    SELECT *
    FROM {{ ref('drive_rentals_fct') }}
),

cities AS (
    SELECT *
    FROM {{ ref('drive_cities_dim') }}
)

SELECT
    -- IDs
    vehicle_id,
    model_id,
    fleet_id,
    -- date
    end_date AS date,
    -- vehicle dimensions
    vehicle_city,
    model_name,
    brand,
    energy_type,
    vehicle_segment,
    vehicle_seats,
    fleet_name,
    company_type,
    -- utilization metrics
    COUNT(DISTINCT rental_id) AS rentals_count,
    SUM(rental_duration_min) AS total_rental_duration_min,
    SUM(rental_duration_hour) AS total_rental_duration_hour,
    SUM(distance_km) AS total_distance_km,
    -- revenue metrics
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_revenue) AS net_revenue,
    -- incidents and support
    SUM(incident_count) AS incident_count,
    SUM(critical_incident_count) AS critical_incident_count,
    SUM(ticket_count) AS ticket_count,

FROM rentals AS r 
GROUP BY 
    vehicle_id, model_id, fleet_id, date, vehicle_city, 
    model_name, brand, energy_type, vehicle_segment, vehicle_seats, fleet_name, company_type
