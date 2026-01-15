WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_reservation_raw') }}
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
        SAFE_CAST(reservation_id AS STRING) AS reservation_id,
        SAFE_CAST(user_id AS STRING) AS user_id,
        SAFE_CAST(vehicle_id AS STRING) AS vehicle_id,
        SAFE_CAST(city_id AS STRING) AS city_id,
        SAFE_CAST(package_id AS STRING) AS package_id,
        SAFE_CAST(rental_id AS STRING) AS rental_id,
        SAFE_CAST(status AS STRING) AS status,
        SAFE_CAST(cancellation_reason AS STRING) AS cancellation_reason,
        {{ cast_iso_datetimes(['reserved_at', 'reservation_start_at', 'reservation_end_at', 'cancelled_at', 'created_at', 'updated_at']) }},
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        reservation_id,
        user_id,
        vehicle_id,
        city_id,
        package_id,
        reserved_at,
        reservation_start_at,
        reservation_end_at,
        {{ standardize_string('status') }} AS status,
        {{ standardize_string('cancellation_reason') }} AS cancellation_reason,
        cancelled_at,
        rental_id,
        created_at,
        updated_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization