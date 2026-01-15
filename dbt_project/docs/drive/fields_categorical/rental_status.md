{% docs rental_status %}

### Rental Status
Represents the current lifecycle state of a rental.

### Possible Values
| Status | Meaning |
|------|--------|
| `ongoing` | Rental is currently in progress |
| `completed` | Rental ended successfully |
| `cancelled` | Rental was cancelled before completion |

### Business Rules
- A rental can only be `completed` after a valid end event
- `cancelled` rentals do not generate revenue

### Usage Notes
- Use `completed` rentals as the default analytical population
- Exclude `cancelled` rentals from revenue and utilization metrics

{% enddocs %}
