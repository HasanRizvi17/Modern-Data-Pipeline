{% macro limit_data_in_dev(column_name, source_relation, dev_days_back=30) %}
{%- if target.name == 'dev' %}
{%- set max_ts %}(SELECT MAX(cast({{ column_name }} as {{ dbt.type_timestamp() }})) FROM {{ source_relation }}){%- endset %}
WHERE cast({{ column_name }} as {{ dbt.type_timestamp() }}) >= cast({{ dbt.dateadd('day', -1 * dev_days_back, max_ts) }} as {{ dbt.type_timestamp() }})
{%- endif %}
{% endmacro %}
