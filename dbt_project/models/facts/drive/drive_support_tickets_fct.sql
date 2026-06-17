WITH

ticket_lifecycle AS (
    SELECT *
    FROM {{ ref('drive_ticket_lifecycle_int') }}
)

SELECT
    * EXCEPT(ticket_created_at, ticket_updated_at, ticket_has_escalation),
    TIMESTAMP_DIFF(first_response_at, ticket_created_at, MINUTE) AS first_response_time_minutes,
    TIMESTAMP_DIFF(first_response_at, ticket_created_at, HOUR) AS first_response_time_hours,
    TIMESTAMP_DIFF(resolved_at, ticket_created_at, MINUTE) AS resolution_time_minutes,
    TIMESTAMP_DIFF(resolved_at, ticket_created_at, HOUR) AS resolution_time_hours,
    CASE WHEN TIMESTAMP_DIFF(resolved_at, ticket_created_at, HOUR) <= 24 THEN TRUE ELSE FALSE END AS resolved_within_24hrs,
    ticket_created_at, 
    ticket_updated_at
FROM ticket_lifecycle


