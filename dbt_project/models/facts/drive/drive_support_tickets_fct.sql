WITH

tickets AS (
    SELECT *
    FROM {{ ref('drive_support_ticket_stg') }}
),

ticket_history AS (
    SELECT *
    FROM {{ ref('drive_ticket_history_stg') }}
),

ticket_lifecycle AS (
    SELECT
        t.ticket_id,
        t.rental_id,
        t.user_id,
        CASE WHEN t.rental_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_linked_to_rental,
        t.category,
        t.subject,
        t.description,
        t.priority,
        t.status,
        t.channel,
        t.satisfaction_rating AS ticket_support_rating,
        SUM(CASE WHEN th.new_value = 'reopened' THEN 1 ELSE 0 END) AS reopen_count,
        SUM(CASE WHEN th.new_value = 'escalated' THEN 1 ELSE 0 END) AS escalation_count,
        MIN(CASE WHEN th.new_value = 'in_progress' THEN th.changed_at END) AS first_response_at,
        MIN(CASE WHEN th.new_value = 'escalated' THEN th.changed_at END) AS first_escalated_at,
        MAX(CASE WHEN th.new_value = 'resolved' THEN th.changed_at END) AS resolved_at,
        t.created_at AS ticket_created_at,
        t.updated_at AS ticket_updated_at
    FROM tickets AS t
    LEFT JOIN ticket_history AS th ON t.ticket_id = th.ticket_id
    WHERE th.field_changed = 'status'
    GROUP BY 
        t.ticket_id, t.rental_id, t.user_id, t.category, t.subject, t.description, t.priority, t.status, t.channel, 
        ticket_support_rating, ticket_created_at, ticket_updated_at
)

SELECT
    * EXCEPT(ticket_created_at, ticket_updated_at),
    TIMESTAMP_DIFF(first_response_at, ticket_created_at, MINUTE) AS first_response_time_minutes,
    TIMESTAMP_DIFF(first_response_at, ticket_created_at, HOUR) AS first_response_time_hours,
    TIMESTAMP_DIFF(resolved_at, ticket_created_at, MINUTE) AS resolution_time_minutes,
    TIMESTAMP_DIFF(resolved_at, ticket_created_at, HOUR) AS resolution_time_hours,
    CASE WHEN TIMESTAMP_DIFF(resolved_at, ticket_created_at, HOUR) <= 24 THEN TRUE ELSE FALSE END AS resolved_within_24hrs,
    ticket_created_at, 
    ticket_updated_at
FROM ticket_lifecycle


