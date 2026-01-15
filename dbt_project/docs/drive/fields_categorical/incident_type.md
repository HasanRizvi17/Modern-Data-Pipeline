{% docs incident_type %}

### Incident Type
Categorizes the nature of an incident associated with a rental.

### Possible Values
| Type | Meaning |
|------|--------|
| `accident` | Collision or accident involving the vehicle |
| `damage` | Physical damage without a collision |
| `theft` | Vehicle theft or attempted theft |
| `breakdown` | Mechanical or technical failure |
| `other` | Incident not covered by standard categories |

### Business Rules
- Type is assigned at incident creation
- `other` should be used only when no standard category applies

### Usage Notes
- Use incident type for root-cause and fleet risk analysis
- Review usage of `other` regularly for categorization gaps

{% enddocs %}
