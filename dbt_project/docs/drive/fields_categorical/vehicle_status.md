{% docs vehicle_status %}

### Vehicle Status
Represents the current operational state of a vehicle.

### Possible Values
| Status | Meaning |
|------|--------|
| `active` | Vehicle is available for rental |
| `maintenance` | Vehicle is temporarily unavailable |
| `decommissioned` | Vehicle permanently removed from service |

### Business Rules
- Only `active` vehicles can be rented
- `decommissioned` is a terminal state

### Usage Notes
- Exclude non-active vehicles from availability metrics
- Track maintenance impact using status transitions

{% enddocs %}
