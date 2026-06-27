{% set raw_source = source('drive_raw', 'drive_reservation_raw') %}

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
            {'name': 'reservation_id', 'path': '$.id'},
            {'name': 'user_id', 'path': '$.user_id'},
            {'name': 'vehicle_id', 'path': '$.vehicle_id'},
            {'name': 'city_id', 'path': '$.city_id'},
            {'name': 'package_id', 'path': '$.package_id'},
            {'name': 'reserved_at', 'path': '$.reserved_at'},
            {'name': 'reservation_start_at', 'path': '$.reservation_start_at'},
            {'name': 'reservation_end_at', 'path': '$.reservation_end_at'},
            {'name': 'status', 'path': '$.status'},
            {'name': 'cancellation_reason', 'path': '$.cancellation_reason'},
            {'name': 'cancelled_at', 'path': '$.cancelled_at'},
            {'name': 'rental_id', 'path': '$.rental_id'},
            {'name': 'created_at', 'path': '$.created_at'},
            {'name': 'updated_at', 'path': '$.updated_at'}
        ]) }},
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        {{ dbt.safe_cast('reservation_id', dbt.type_string()) }} AS reservation_id,
        {{ dbt.safe_cast('user_id', dbt.type_string()) }} AS user_id,
        {{ dbt.safe_cast('vehicle_id', dbt.type_string()) }} AS vehicle_id,
        {{ dbt.safe_cast('city_id', dbt.type_string()) }} AS city_id,
        {{ dbt.safe_cast('package_id', dbt.type_string()) }} AS package_id,
        {{ dbt.safe_cast('rental_id', dbt.type_string()) }} AS rental_id,
        {{ dbt.safe_cast('status', dbt.type_string()) }} AS status,
        {{ dbt.safe_cast('cancellation_reason', dbt.type_string()) }} AS cancellation_reason,
        {{ cast_iso_datetimes(['reserved_at', 'reservation_start_at', 'reservation_end_at', 'cancelled_at', 'created_at', 'updated_at']) }},
        {{ cast_ingestion_timestamp('ingestion_timestamp') }} AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        -- IDs
        reservation_id,
        user_id,
        vehicle_id,
        city_id,
        package_id,
        rental_id,
        -- attributes
        {{ standardize_string('status') }} AS status,
        {{ standardize_string('cancellation_reason') }} AS cancellation_reason,
        -- timestamps
        reserved_at,
        reservation_start_at,
        reservation_end_at,
        cancelled_at,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization