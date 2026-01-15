{% docs support_ticket_status %}

### Support Ticket Status
Represents the current processing state of a support ticket.

### Possible Values
| Status | Meaning |
|------|--------|
| `open` | Ticket has been created |
| `in_progress` | Ticket is being worked on |
| `escalated` | Ticket has been escalated |
| `resolved` | Issue has been resolved |
| `closed` | Ticket has been closed |

### Business Rules
- Tickets move sequentially through defined states
- `closed` is a terminal state

### Usage Notes
- Use `open` and `in_progress` to track backlog
- Measure resolution time using `resolved` timestamp

{% enddocs %}
