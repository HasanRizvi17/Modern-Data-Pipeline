WITH

incidents AS (
    SELECT *
    FROM {{ ref('drive_incidents_fct') }}
),

rentals AS (
    SELECT *
    FROM {{ ref('drive_rentals_fct') }}
),

dates AS (
    SELECT date
    FROM {{ ref('dates_dim') }}
    WHERE date <= CURRENT_DATE()
),

-- date x date_type spine, so zero-activity dates appear for both the
-- created-date and resolved-date views instead of being silently absent
date_type_spine AS (
    SELECT d.date, dt AS date_type
    FROM dates AS d
    CROSS JOIN UNNEST(['created', 'resolved']) AS dt
),

-- total rentals per day (for incident rate calculations in BI)
daily_rentals AS (
    SELECT
        created_date AS date,
        COUNT(*) AS total_rentals
    FROM rentals
    GROUP BY created_date
),

-- KPIs by incident creation date
kpis_by_created_date AS (
    SELECT
        'created' AS date_type,
        DATE(i.created_at) AS date,
        -- incident dimensions
        i.incident_type,
        i.severity,
        i.police_report_filed,
        -- volume
        COUNT(i.incident_id) AS total_incidents,
        COUNTIF(i.resolved_at IS NOT NULL) AS resolved_incidents,
        COUNTIF(i.resolved_at IS NULL) AS open_incidents,
        -- time and cost
        AVG(i.resolution_time_hours) AS avg_resolution_time_hours,
        SUM(i.estimated_cost) AS total_estimated_cost,
        -- context
        COUNT(DISTINCT i.rental_id) AS rentals_with_incidents
    FROM incidents AS i
    GROUP BY date_type, date, i.incident_type, i.severity, i.police_report_filed
),

-- KPIs by incident resolution date
kpis_by_resolved_date AS (
    SELECT
        'resolved' AS date_type,
        DATE(i.resolved_at) AS date,
        -- incident dimensions
        i.incident_type,
        i.severity,
        i.police_report_filed,
        -- volume
        COUNT(i.incident_id) AS total_incidents,
        -- resolution (all resolved by definition)
        COUNTIF(i.resolved_at IS NOT NULL) AS resolved_incidents, -- dummy field to allow union, all tickets here are resolved
        COUNTIF(i.resolved_at IS NULL) AS open_incidents, -- dummy field to allow union, all tickets here are resolved
        -- time and cost
        AVG(i.resolution_time_hours) AS avg_resolution_time_hours,
        SUM(i.estimated_cost) AS total_estimated_cost,
        -- context
        COUNT(DISTINCT i.rental_id) AS rentals_with_incidents
    FROM incidents AS i
    WHERE i.resolved_at IS NOT NULL
    GROUP BY date_type, date, i.incident_type, i.severity, i.police_report_filed
),

-- in the BI tool, we create a parameter to allow end-data-users to switch between created_date and resolved_date views
kpis_unioned AS (
    SELECT * FROM kpis_by_created_date
    UNION ALL
    SELECT * FROM kpis_by_resolved_date
)

SELECT
    s.date_type,
    s.date,
    k.incident_type,
    k.severity,
    k.police_report_filed,
    COALESCE(k.total_incidents, 0) AS total_incidents,
    COALESCE(k.resolved_incidents, 0) AS resolved_incidents,
    COALESCE(k.open_incidents, 0) AS open_incidents,
    k.avg_resolution_time_hours,
    k.total_estimated_cost,
    COALESCE(k.rentals_with_incidents, 0) AS rentals_with_incidents,
    COALESCE(dr.total_rentals, 0) AS total_rentals, -- total rentals on date-level (used to calculate contact rate in the BI tool)
    CURRENT_TIMESTAMP() AS loaded_at
FROM date_type_spine AS s
LEFT JOIN kpis_unioned AS k ON s.date_type = k.date_type AND s.date = k.date
LEFT JOIN daily_rentals AS dr ON s.date = dr.date
