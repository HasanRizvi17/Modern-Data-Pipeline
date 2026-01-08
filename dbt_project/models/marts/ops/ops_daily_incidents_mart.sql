WITH

incidents AS (
    SELECT *
    FROM {{ ref('drive_incidents_fct') }}
),

rentals AS (
    SELECT *
    FROM {{ ref('drive_rentals_fct') }}
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
        COUNT(DISTINCT i.rental_id) AS rentals_with_incidents,
        dr.total_rentals -- total rentals on date-level (used to calculate contact rate in the BI tool)
    FROM incidents AS i
    LEFT JOIN daily_rentals AS dr ON DATE(i.created_at) = dr.date
    GROUP BY date_type, date, i.incident_type, i.severity, i.police_report_filed, dr.total_rentals
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
        COUNT(DISTINCT i.rental_id) AS rentals_with_incidents,
        dr.total_rentals
    FROM incidents AS i
    LEFT JOIN daily_rentals AS dr
        ON DATE(i.resolved_at) = dr.date
    WHERE i.resolved_at IS NOT NULL
    GROUP BY date_type, date, i.incident_type, i.severity, i.police_report_filed, dr.total_rentals
)

-- in the BI tool, we create a parameter to allow end-data-users to switch between created_date and closed_date views
SELECT *
FROM kpis_by_created_date
UNION ALL
SELECT *
FROM kpis_by_resolved_date
