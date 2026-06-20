WITH

ticket_lifecycle AS (
    SELECT *
    FROM {{ ref('drive_ticket_lifecycle_int') }}
),

sla_targets AS (
    SELECT *
    FROM {{ ref('support_ticket_sla_targets') }}
)

SELECT
    t.* EXCEPT(ticket_created_at, ticket_updated_at, ticket_has_escalation),
    TIMESTAMP_DIFF(first_response_at, ticket_created_at, MINUTE) AS first_response_time_minutes,
    TIMESTAMP_DIFF(first_response_at, ticket_created_at, HOUR) AS first_response_time_hours,
    TIMESTAMP_DIFF(resolved_at, ticket_created_at, MINUTE) AS resolution_time_minutes,
    TIMESTAMP_DIFF(resolved_at, ticket_created_at, HOUR) AS resolution_time_hours,
    sla.target_hours AS sla_target_hours,
    CASE
        WHEN resolved_at IS NULL THEN FALSE
        WHEN TIMESTAMP_DIFF(resolved_at, ticket_created_at, HOUR) <= sla.target_hours THEN TRUE
        ELSE FALSE
    END AS resolved_within_sla,
    ticket_created_at,
    ticket_updated_at
FROM ticket_lifecycle AS t
LEFT JOIN sla_targets AS sla ON t.priority = sla.priority


