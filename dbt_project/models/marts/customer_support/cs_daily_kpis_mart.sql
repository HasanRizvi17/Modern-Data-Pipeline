WITH

tickets AS (
    SELECT *
    FROM {{ ref('drive_support_tickets_fct') }}
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
        DATE(end_date) AS date,
        COUNT(*) AS total_rentals
    FROM rentals
    GROUP BY date
),

-- modeling KPIs for tickets aggregated by creation date
kpis_by_created_date AS (
    SELECT
        'created' AS date_type,
        DATE(t.ticket_created_at) AS date,
        -- dimensions
        t.is_linked_to_rental,
        t.user_city,
        t.category,
        t.subject,
        t.priority,
        t.channel,
        -- metrics
        COUNT(t.ticket_id) AS total_tickets,
        SUM(CASE WHEN t.status = 'resolved' THEN 1 ELSE 0 END) AS resolved_tickets,
        SUM(CASE WHEN t.status != 'resolved' THEN 1 ELSE 0 END) AS open_tickets,
        SUM(reopen_count) AS reopen_count,
        SUM(escalation_count) AS escalation_count,
        AVG(first_response_time_hours) AS avg_first_response_time_hours,
        AVG(resolution_time_hours) AS avg_resolution_time_hours,
        SUM(CASE WHEN resolved_within_sla THEN 1 ELSE 0 END) AS tickets_resolved_within_sla,
        AVG(t.ticket_support_rating) AS avg_ticket_support_rating
    FROM tickets AS t
    GROUP BY date_type, date, t.is_linked_to_rental, t.user_city, t.category, t.subject, t.priority, t.channel
),

-- modeling KPIs for tickets aggregated by resolution date
kpis_by_resolved_date AS (
    SELECT
        'resolved' AS date_type,
        DATE(t.resolved_at) AS date,
        -- dimensions
        t.is_linked_to_rental,
        t.user_city,
        t.category,
        t.subject,
        t.priority,
        t.channel,
        -- metrics
        COUNT(t.ticket_id) AS total_tickets,
        SUM(CASE WHEN t.status = 'resolved' THEN 1 ELSE 0 END) AS resolved_tickets, -- dummy field to allow union, all tickets here are resolved
        SUM(CASE WHEN t.status != 'resolved' THEN 1 ELSE 0 END) AS open_tickets, -- dummy field to allow union, all tickets here are resolved
        SUM(reopen_count) AS reopen_count,
        SUM(escalation_count) AS escalation_count,
        AVG(first_response_time_hours) AS avg_first_response_time_hours,
        AVG(resolution_time_hours) AS avg_resolution_time_hours,
        SUM(CASE WHEN resolved_within_sla THEN 1 ELSE 0 END) AS tickets_resolved_within_sla,
        AVG(t.ticket_support_rating) AS avg_ticket_support_rating
    FROM tickets AS t
    WHERE t.resolved_at IS NOT NULL
    GROUP BY date_type, date, t.is_linked_to_rental, t.user_city, t.category, t.subject, t.priority, t.channel
),

-- in the BI tool, we create a parameter to allow end-data-users to switch between created_date and resolved_date views
kpis_unioned AS (
    SELECT * FROM kpis_by_created_date
    UNION ALL
    SELECT * FROM kpis_by_resolved_date
),

-- daily ticket volume independent of the dimension breakdown above, used as
-- the base for the rolling 7d ticket volume metric
daily_ticket_totals AS (
    SELECT
        s.date_type,
        s.date,
        COALESCE(SUM(k.total_tickets), 0) AS daily_total_tickets
    FROM date_type_spine AS s
    LEFT JOIN kpis_unioned AS k ON s.date_type = k.date_type AND s.date = k.date
    GROUP BY s.date_type, s.date
),

rolling_ticket_volume AS (
    SELECT
        date_type,
        date,
        SUM(daily_total_tickets) OVER (
            PARTITION BY date_type ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_ticket_volume
    FROM daily_ticket_totals
)

SELECT
    s.date_type,
    s.date,
    k.is_linked_to_rental,
    k.user_city,
    k.category,
    k.subject,
    k.priority,
    k.channel,
    COALESCE(k.total_tickets, 0) AS total_tickets,
    COALESCE(k.resolved_tickets, 0) AS resolved_tickets,
    COALESCE(k.open_tickets, 0) AS open_tickets,
    COALESCE(k.reopen_count, 0) AS reopen_count,
    COALESCE(k.escalation_count, 0) AS escalation_count,
    k.avg_first_response_time_hours,
    k.avg_resolution_time_hours,
    COALESCE(k.tickets_resolved_within_sla, 0) AS tickets_resolved_within_sla,
    k.avg_ticket_support_rating,
    COALESCE(dr.total_rentals, 0) AS total_rentals, -- total rentals on date-level (used to calculate contact rate in the BI tool)
    rtv.rolling_7d_ticket_volume,
    CURRENT_TIMESTAMP() AS loaded_at
FROM date_type_spine AS s
LEFT JOIN kpis_unioned AS k ON s.date_type = k.date_type AND s.date = k.date
LEFT JOIN daily_rentals AS dr ON s.date = dr.date
LEFT JOIN rolling_ticket_volume AS rtv ON s.date_type = rtv.date_type AND s.date = rtv.date
