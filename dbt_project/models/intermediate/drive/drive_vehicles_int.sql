WITH

vehicles AS (
    SELECT *
    FROM {{ ref('drive_vehicle_stg') }}
),

vehicle_models AS (
    SELECT *
    FROM {{ ref('drive_vehicle_model_stg') }}
),

vehicle_fleets AS (
    SELECT *
    FROM {{ ref('drive_vehicle_fleet_stg') }}
),

vehicles_data AS (
    SELECT
        v.vehicle_id,
        v.model_id,
        v.fleet_id,
        v.city_id,
        v.status AS vehicle_status,
        v.fuel_level,
        v.battery_level,
        vm.model_name,
        vm.brand,
        vm.energy_type,
        vm.segment AS vehicle_segment,
        vm.seats AS vehicle_seats,
        vf.fleet_name,
        vf.company_type,
        CASE
            WHEN v.status = 'active' THEN TRUE
            ELSE FALSE
        END AS is_active_vehicle,
        v.created_at,
        DATE(v.created_at) AS created_date,
        v.updated_at,
        v.ingestion_timestamp
    FROM vehicles AS v
    LEFT JOIN vehicle_models AS vm ON v.model_id = vm.model_id
    LEFT JOIN vehicle_fleets AS vf ON v.fleet_id = vf.fleet_id
)

SELECT *
FROM vehicles_data
