{% docs user_status %}

### User Status
Represents the current state of a user account based on backend logic.

### Possible Values
| Status | Meaning |
|------|--------|
| `registered` | User completed registration but has not yet been validated |
| `validated` | User’s driver license has been validated |
| `active` | User has successfully completed at least one one rental since registration |
| `banned` | User is temporarily banned |
| `deleted` | User account has been permanently deleted |

### Business Rules
- Status transitions are controlled by backend logic
- `active` status is evaluated on a rolling 7-day window
- `banned` users must be excluded from all user-facing metrics
- `deleted` is a terminal state

### Usage Notes
- Do not include `deleted` users in cohort or retention analysis
- `validated` does not imply activity

{% enddocs %}
