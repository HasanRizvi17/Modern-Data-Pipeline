WITH

support_tickets AS (
    SELECT *
    FROM {{ ref('drive_support_ticket_stg') }}
),

ticket_history AS (
    SELECT *
    FROM {{ ref('drive_ticket_history_stg') }}
),

incidents AS (
    SELECT *
    FROM {{ ref('drive_incident_stg') }}
),

tickets_aggregated AS (
    SELECT
        t.rental_id AS rental_id,
        ROUND(AVG(satisfaction_rating), 2) AS ticket_support_rating,
        COUNT(*) AS ticket_count,
        MIN(t.created_at) AS first_ticket_created_at,
        MAX(t.created_at) AS last_ticket_created_at
    FROM support_tickets AS t
    WHERE t.rental_id IS NOT NULL
    GROUP BY t.rental_id
),

ticket_logs AS (
    SELECT
        t.rental_id,
        MAX(CASE WHEN th.new_value = 'escalated' THEN TRUE ELSE FALSE END) AS ticket_has_escalation,
        MIN(CASE WHEN th.new_value = 'in_progress' THEN changed_at ELSE NULL END) AS ticket_first_response_at,
        MIN(CASE WHEN th.new_value = 'escalated' THEN changed_at ELSE NULL END) AS ticket_first_escalated_at,
        MAX(CASE WHEN th.new_value = 'resolved' THEN changed_at ELSE NULL END) AS ticket_resolved_at
    FROM ticket_history AS th
    LEFT JOIN support_tickets AS t ON t.ticket_id = th.ticket_id
    WHERE th.field_changed = 'status'
    GROUP BY t.rental_id
),

incidents_aggregated AS (
    SELECT
        i.rental_id AS rental_id,
        COUNT(*) AS incident_count,
        SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_incident_count,
        SUM(CASE WHEN police_report_filed = TRUE THEN 1 ELSE 0 END) AS incidents_with_police_report_filed,
        SUM(estimated_cost) AS incident_estimated_cost,
        MIN(reported_at) AS first_incident_reported_at,
        MAX(reported_at) AS last_incident_reported_at
    FROM incidents AS i
    GROUP BY i.rental_id
),

issues_by_rental AS (
    SELECT
        COALESCE(ta.rental_id, i.rental_id) AS rental_id,
        -- support tickets
        ta.ticket_support_rating,
        COALESCE(ta.ticket_count, 0) AS ticket_count,
        ta.first_ticket_created_at,
        ta.last_ticket_created_at,
        tl.ticket_first_response_at,
        tl.ticket_first_escalated_at,
        tl.ticket_resolved_at,
        tl.ticket_has_escalation,
        CASE
            WHEN COALESCE(ta.ticket_count, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS has_support_ticket,
        -- incidents
        COALESCE(incident_count, 0) AS incident_count,
        COALESCE(critical_incident_count, 0) AS critical_incident_count,
        COALESCE(incidents_with_police_report_filed, 0) AS incidents_with_police_report_filed,
        incident_estimated_cost,
        first_incident_reported_at,
        last_incident_reported_at,
        CASE
            WHEN COALESCE(i.incident_count, 0) > 0 THEN TRUE
            ELSE FALSE
        END AS has_incident
    FROM tickets_aggregated AS ta
    LEFT JOIN ticket_logs AS tl ON ta.rental_id = tl.rental_id
    FULL JOIN incidents_aggregated AS i ON i.rental_id = ta.rental_id
)

SELECT *
FROM issues_by_rental