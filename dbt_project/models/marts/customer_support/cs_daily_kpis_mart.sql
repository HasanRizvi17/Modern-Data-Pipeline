WITH

tickets AS (
    SELECT *
    FROM {{ ref('drive_support_tickets_fct') }}
),

rentals AS (
    SELECT *
    FROM {{ ref('drive_rentals_fct') }}
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
        t.is_linked_to_rental,
        t.category,
        t.subject,
        t.priority,
        t.channel,
        COUNT(t.ticket_id) AS total_tickets,
        SUM(CASE WHEN t.status = 'resolved' THEN 1 ELSE 0 END) AS resolved_tickets,
        SUM(CASE WHEN t.status != 'resolved' THEN 1 ELSE 0 END) AS open_tickets,
        SUM(reopen_count) AS reopen_count,
        SUM(escalation_count) AS escalation_count,
        AVG(first_response_time_hours) AS avg_first_response_time_hours,
        AVG(resolution_time_hours) AS avg_resolution_time_hours,
        SUM(CASE WHEN resolved_within_24hrs THEN 1 ELSE 0 END) AS tickets_resolved_within_24hrs,
        AVG(t.ticket_support_rating) AS avg_ticket_support_rating,
        dr.total_rentals, -- total rentals on date-level (used to calculate contact rate in the BI tool)
    FROM tickets AS t
    LEFT JOIN daily_rentals AS dr ON DATE(t.ticket_created_at) = dr.date
    GROUP BY date_type, date, t.is_linked_to_rental, t.category, t.subject, t.priority, t.channel, dr.total_rentals
),

-- modeling KPIs for tickets aggregated by resolution date
kpis_by_closed_date AS (
    SELECT
        'closed' AS date_type,
        DATE(t.resolved_at) AS date,
        t.is_linked_to_rental,
        t.category,
        t.subject,
        t.priority,
        t.channel,
        COUNT(t.ticket_id) AS total_tickets,
        SUM(CASE WHEN t.status = 'resolved' THEN 1 ELSE 0 END) AS resolved_tickets, -- dummy field to allow union, all tickets here are resolved
        SUM(CASE WHEN t.status != 'resolved' THEN 1 ELSE 0 END) AS open_tickets, -- dummy field to allow union, all tickets here are resolved
        SUM(reopen_count) AS reopen_count,
        SUM(escalation_count) AS escalation_count,
        AVG(first_response_time_hours) AS avg_first_response_time_hours,
        AVG(resolution_time_hours) AS avg_resolution_time_hours,
        SUM(CASE WHEN resolved_within_24hrs THEN 1 ELSE 0 END) AS tickets_resolved_within_24hrs,
        AVG(t.ticket_support_rating) AS avg_ticket_support_rating,
        dr.total_rentals, -- total rentals on date-level (used to calculate contact rate in the BI tool)
    FROM tickets AS t
    LEFT JOIN daily_rentals AS dr ON DATE(t.resolved_at) = dr.date
    GROUP BY date_type, date, t.is_linked_to_rental, t.category, t.subject, t.priority, t.channel, dr.total_rentals
)

-- in the BI tool, we create a parameter to allow end-data-users to switch between created_date and closed_date views
SELECT *
FROM kpis_by_created_date
UNION ALL
SELECT *
FROM kpis_by_closed_date