{% docs vehicle_fleet_company_type %}

### Vehicle Fleet Company Type
Represents the ownership model of a vehicle fleet.

### Possible Values
| Type | Meaning |
|------|--------|
| `owned` | Vehicles owned by the company |
| `leased` | Vehicles leased from a third party |
| `partner` | Vehicles provided by a partner |

### Business Rules
- Company type is assigned at fleet creation
- A fleet can have only one company type

### Usage Notes
- Segment fleet cost analysis by company type
- Compare utilization across ownership models

{% enddocs %}
