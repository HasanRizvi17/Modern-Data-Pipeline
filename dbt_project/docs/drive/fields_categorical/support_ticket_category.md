{% docs support_ticket_category %}

### Support Ticket Category
Classifies the primary issue reported in a support ticket.

### Possible Values
| Category | Meaning |
|---------|--------|
| `billing` | Billing or payment-related issue |
| `vehicle_issue` | Issue related to vehicle condition or behavior |
| `account` | Account or profile-related issue |
| `booking` | Reservation or booking-related issue |
| `incident` | Incident-related inquiry |
| `app_bug` | Application or technical bug |
| `other` | Issue not covered by standard categories |

### Business Rules
- Category is assigned at ticket creation
- `other` should be used sparingly

### Usage Notes
- Use categories to identify common support drivers
- Regularly review `other` usage for categorization improvements

{% enddocs %}
