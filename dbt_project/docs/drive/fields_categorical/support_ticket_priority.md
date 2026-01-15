{% docs support_ticket_priority %}

### Support Ticket Priority
Indicates the urgency level assigned to a support ticket.

### Possible Values
| Priority | Meaning |
|---------|--------|
| `low` | Minor issue with no immediate impact |
| `medium` | Standard issue requiring attention |
| `high` | Issue impacting user experience |
| `critical` | Severe issue requiring immediate action |

### Business Rules
- Priority may be updated by support agents
- `critical` tickets trigger escalation workflows

### Usage Notes
- Use priority when measuring SLA compliance
- Analyze `high` and `critical` tickets separately

{% enddocs %}
