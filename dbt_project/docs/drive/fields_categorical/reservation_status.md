{% docs reservation_status %}

### Reservation Status
Represents the lifecycle state of a vehicle reservation.

### Possible Values
| Status | Meaning |
|------|--------|
| `pending` | Reservation has been created but not confirmed |
| `confirmed` | Reservation has been confirmed |
| `cancelled` | Reservation was cancelled by the user or system |
| `expired` | Reservation expired without conversion |
| `converted_to_rental` | Reservation was converted into a rental |

### Business Rules
- Only `converted_to_rental` reservations result in rentals
- `expired` reservations occur after a timeout period

### Usage Notes
- Use reservation status to analyze conversion funnels
- Exclude `cancelled` and `expired` reservations from demand metrics

{% enddocs %}
