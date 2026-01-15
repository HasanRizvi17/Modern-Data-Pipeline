{% docs incident_status %}

### Incident Status
Represents the current processing state of an incident.

### Possible Values
| Status | Meaning |
|------|--------|
| `reported` | Incident has been reported by the user or system |
| `under_review` | Incident is being reviewed by operations or support |
| `resolved` | Incident has been resolved |
| `closed` | Incident has been formally closed |
| `insurance_claimed` | Incident resulted in an insurance claim |

### Business Rules
- Incidents progress sequentially through defined states
- `closed` and `insurance_claimed` are terminal states

### Usage Notes
- Use `reported` and `under_review` to track active incident backlog
- Exclude `closed` incidents from operational SLA metrics

{% enddocs %}
