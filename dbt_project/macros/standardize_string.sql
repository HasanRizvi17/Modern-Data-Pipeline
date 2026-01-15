{% macro standardize_string(col, lower='yes', nullif_empty='yes') %}
    {%- if lower == 'yes' and nullif_empty == 'yes' %}
        NULLIF(LOWER(TRIM({{ col }})), '')
    {%- elif lower == 'yes' and nullif_empty == 'no' %}
        LOWER(TRIM({{ col }}))
    {%- elif lower == 'no' and nullif_empty == 'yes' %}
        NULLIF(TRIM({{ col }}), '')
    {%- else %}
        TRIM({{ col }})
    {%- endif %}
{% endmacro %}
