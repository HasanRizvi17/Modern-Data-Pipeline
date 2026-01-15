{% docs payment_status %}

### Payment Status
Represents the processing outcome of a payment transaction.

### Possible Values
| Status | Meaning |
|------|--------|
| `succeeded` | Payment was successfully processed |
| `failed` | Payment attempt failed |
| `refunded` | Payment was refunded partially or fully |

### Business Rules
- Only `succeeded` payments contribute to revenue
- `refunded` payments must be linked to a successful payment

### Usage Notes
- Use `succeeded` payments for financial reporting
- Track `failed` payments separately for payment reliability analysis

{% enddocs %}
