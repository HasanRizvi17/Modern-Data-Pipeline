{% set raw_source = source('drive_raw', 'drive_rental_raw') %}

WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ raw_source }}
    {{ limit_data_in_dev('timestamp', raw_source) }}
),

extraction AS (
    SELECT
        {{ json_extract_fields('data', [
            {'name': 'rental_id', 'path': '$.id'},
            {'name': 'user_id', 'path': '$.user_id'},
            {'name': 'vehicle_id', 'path': '$.vehicle_id'},
            {'name': 'package_id', 'path': '$.package_id'},
            {'name': 'start_city_id', 'path': '$.start_city_id'},
            {'name': 'end_city_id', 'path': '$.end_city_id'},
            {'name': 'start_time', 'path': '$.start_time'},
            {'name': 'end_time', 'path': '$.end_time'},
            {'name': 'status', 'path': '$.status'},
            {'name': 'distance_km', 'path': '$.distance_km'},
            {'name': 'rental_cost', 'path': '$.final_cost'},
            {'name': 'promo_id', 'path': '$.promo_id'},
            {'name': 'reservation_id', 'path': '$.reservation_id'},
            {'name': 'incident_id', 'path': '$.incident_id'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        {{ dbt.safe_cast('rental_id', dbt.type_string()) }} AS rental_id,
        {{ dbt.safe_cast('user_id', dbt.type_string()) }} AS user_id,
        {{ dbt.safe_cast('vehicle_id', dbt.type_string()) }} AS vehicle_id,
        {{ dbt.safe_cast('package_id', dbt.type_string()) }} AS package_id,
        {{ dbt.safe_cast('start_city_id', dbt.type_string()) }} AS start_city_id,
        {{ dbt.safe_cast('end_city_id', dbt.type_string()) }} AS end_city_id,
        {{ dbt.safe_cast('status', dbt.type_string()) }} AS status,
        {{ dbt.safe_cast('distance_km', dbt.type_float()) }} AS distance_km,
        {{ dbt.safe_cast('rental_cost', dbt.type_float()) }} AS rental_cost,
        {{ dbt.safe_cast('promo_id', dbt.type_string()) }} AS promo_id,
        {{ dbt.safe_cast('reservation_id', dbt.type_string()) }} AS reservation_id,
        {{ dbt.safe_cast('incident_id', dbt.type_string()) }} AS incident_id,
        {{ cast_iso_datetimes(['start_time', 'end_time', 'created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        rental_id,
        user_id,
        vehicle_id,
        package_id,
        start_city_id,
        end_city_id,
        promo_id,
        reservation_id,
        incident_id,
        -- attributes
        start_time,
        end_time,
        {{ standardize_string('status') }} AS status,
        COALESCE(distance_km, 0) AS distance_km,
        COALESCE(rental_cost, 0) AS rental_cost,
        -- timestamps
        created_at,
        DATE(created_at) AS created_date,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization