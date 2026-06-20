WITH

tickets AS (
    SELECT *
    FROM {{ ref('drive_support_ticket_stg') }}
),

ticket_history AS (
    SELECT *
    FROM {{ ref('drive_ticket_history_stg') }}
),

users AS (
    SELECT *
    FROM {{ ref('drive_users_int') }}
),

ticket_lifecycle AS (
    SELECT
        t.ticket_id,
        t.rental_id,
        t.user_id,
        u.city_id AS user_city_id,
        u.city_name AS user_city,
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
        MAX(CASE WHEN th.new_value = 'escalated' THEN TRUE ELSE FALSE END) AS ticket_has_escalation,
        MIN(CASE WHEN th.new_value = 'in_progress' THEN th.changed_at END) AS first_response_at,
        MIN(CASE WHEN th.new_value = 'escalated' THEN th.changed_at END) AS first_escalated_at,
        MAX(CASE WHEN th.new_value = 'resolved' THEN th.changed_at END) AS resolved_at,
        t.created_at AS ticket_created_at,
        t.updated_at AS ticket_updated_at
    FROM tickets AS t
    LEFT JOIN users AS u ON t.user_id = u.user_id
    LEFT JOIN ticket_history AS th ON t.ticket_id = th.ticket_id
    WHERE th.field_changed = 'status'
    GROUP BY
        t.ticket_id, t.rental_id, t.user_id, u.city_id, u.city_name, t.category, t.subject, t.description, t.priority,
        t.status, t.channel, ticket_support_rating, ticket_created_at, ticket_updated_at
)

SELECT *
FROM ticket_lifecycle
