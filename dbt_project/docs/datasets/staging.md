{% docs docs_staging_dataset %}

## Staging Layer – Drive

This dataset contains staging tables representing raw entities from the DriveX app and web platforms.

Each staging model:
- Contains **one row per source entity**
- Applies only **row-level transformations** such as:
    - JSON extraction
    - Type casting
    - NULL handling
    - Text standardization
- Preserves source-level granularity and semantics
- Serves as the **foundation for downstream intermediate, fact, and dimension models**

No joins, aggregations, or business logics are applied at this layer.

{% enddocs %}
