{% docs rental_package %}

### Rental Package
Represents the pricing package selected for a rental.

### Possible Values
| Package | Meaning |
|--------|--------|
| `per-minute` | Rental charged per minute |
| `per-hour` | Rental charged per hour |
| `daily` | Rental charged per day |
| `unlimited` | Rental with no usage-based limit |

### Business Rules
- Package selection determines pricing logic
- Packages must be active during the rental start time

### Usage Notes
- Use rental package to segment pricing and revenue analysis
- Compare package performance across markets and user segments

{% enddocs %}
