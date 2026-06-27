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
    {{ dbt.datediff('ticket_created_at', 'first_response_at', 'minute') }} AS first_response_time_minutes,
    {{ dbt.datediff('ticket_created_at', 'first_response_at', 'hour') }} AS first_response_time_hours,
    {{ dbt.datediff('ticket_created_at', 'resolved_at', 'minute') }} AS resolution_time_minutes,
    {{ dbt.datediff('ticket_created_at', 'resolved_at', 'hour') }} AS resolution_time_hours,
    sla.target_hours AS sla_target_hours,
    CASE
        WHEN resolved_at IS NULL THEN FALSE
        WHEN {{ dbt.datediff('ticket_created_at', 'resolved_at', 'hour') }} <= sla.target_hours THEN TRUE
        ELSE FALSE
    END AS resolved_within_sla,
    ticket_created_at,
    ticket_updated_at
FROM ticket_lifecycle AS t
LEFT JOIN sla_targets AS sla ON t.priority = sla.priority


