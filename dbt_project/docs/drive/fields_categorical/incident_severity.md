{% docs incident_severity %}

### Incident Severity
Represents the assessed severity level of an incident reported during a rental.

### Possible Values
| Severity | Meaning |
|--------|--------|
| `minor` | Minor issue with negligible impact |
| `moderate` | Issue requiring attention but not critical |
| `severe` | Serious issue impacting vehicle or user safety |
| `critical` | Critical incident requiring immediate action |

### Business Rules
- Severity is assigned at incident creation and may be updated after review
- `critical` incidents trigger escalation and insurance workflows

### Usage Notes
- Segment incident analysis by severity to prioritize operational actions
- Treat `severe` and `critical` incidents separately in risk reporting

{% enddocs %}
