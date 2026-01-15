{% docs support_ticket_channel %}

### Support Ticket Channel
Represents the channel through which a support ticket was created.

### Possible Values
| Channel | Meaning |
|--------|--------|
| `app` | Ticket created via the mobile application |
| `email` | Ticket created via email |
| `phone` | Ticket created via phone support |
| `chat` | Ticket created via live chat |

### Business Rules
- Channel is determined at ticket creation
- Channel cannot be changed after creation

### Usage Notes
- Use channel data to optimize support operations
- Compare resolution times across channels

{% enddocs %}
