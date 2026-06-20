WITH

ticket_lifecycle AS (
    SELECT *
    FROM {{ ref('drive_ticket_lifecycle_int') }}
),

incidents AS (
    SELECT *
    FROM {{ ref('drive_incident_stg') }}
),

rentals AS (
    SELECT *
    FROM {{ ref('drive_rental_base_int') }}
),

tickets_aggregated AS (
    SELECT
        rental_id,
        ROUND(AVG(ticket_support_rating), 2) AS ticket_support_rating,
        COUNT(*) AS ticket_count,
        MIN(ticket_created_at) AS first_ticket_created_at,
        MAX(ticket_created_at) AS last_ticket_created_at,
        MAX(ticket_has_escalation) AS ticket_has_escalation,
        MIN(first_response_at) AS ticket_first_response_at,
        MIN(first_escalated_at) AS ticket_first_escalated_at,
        MAX(resolved_at) AS ticket_resolved_at
    FROM ticket_lifecycle
    WHERE rental_id IS NOT NULL
    GROUP BY rental_id
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
        r.user_id,
        r.vehicle_id,
        r.start_city_id AS city_id,
        -- support tickets
        ta.ticket_support_rating,
        COALESCE(ta.ticket_count, 0) AS ticket_count,
        ta.first_ticket_created_at,
        ta.last_ticket_created_at,
        ta.ticket_first_response_at,
        ta.ticket_first_escalated_at,
        ta.ticket_resolved_at,
        ta.ticket_has_escalation,
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
    FULL JOIN incidents_aggregated AS i ON i.rental_id = ta.rental_id
    LEFT JOIN rentals AS r ON r.rental_id = COALESCE(ta.rental_id, i.rental_id)
)

SELECT *
FROM issues_by_rental