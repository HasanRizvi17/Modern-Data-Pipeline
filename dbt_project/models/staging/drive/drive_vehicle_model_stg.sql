WITH

raw AS (
    SELECT
        timestamp,
        data
    FROM {{ source('drive_raw', 'drive_vehicle_model_raw') }}
),

extraction AS (
    SELECT
        JSON_EXTRACT_SCALAR(data, '$.id') AS model_id,
        JSON_EXTRACT_SCALAR(data, '$.name') AS model_name,
        JSON_EXTRACT_SCALAR(data, '$.brand') AS brand,
        JSON_EXTRACT_SCALAR(data, '$.energy_type') AS energy_type,
        JSON_EXTRACT_SCALAR(data, '$.segment') AS segment,
        JSON_EXTRACT_SCALAR(data, '$.seats') AS seats,
        JSON_EXTRACT_SCALAR(data, '$.created_at') AS created_at,
        timestamp AS ingestion_timestamp
    FROM raw
),

type_casting AS (
    SELECT
        SAFE_CAST(model_id AS STRING) AS model_id,
        SAFE_CAST(model_name AS STRING) AS model_name,
        SAFE_CAST(brand AS STRING) AS brand,
        SAFE_CAST(energy_type AS STRING) AS energy_type,
        SAFE_CAST(segment AS STRING) AS segment,
        SAFE_CAST(seats AS INT64) AS seats,
        DATETIME(TIMESTAMP(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', created_at)), "Europe/Berlin") AS created_at,
        DATETIME(TIMESTAMP(ingestion_timestamp), "Europe/Berlin") AS ingestion_timestamp
    FROM extraction
),

standardization AS (
    SELECT
        model_id,
        LOWER(TRIM(model_name)) AS model_name,
        LOWER(TRIM(brand)) AS brand,
        LOWER(TRIM(energy_type)) AS energy_type,
        LOWER(TRIM(segment)) AS segment,
        COALESCE(seats, 0) AS seats,
        created_at,
        ingestion_timestamp
    FROM type_casting
)

SELECT *
FROM standardization