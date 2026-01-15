{% macro json_extract_fields(json_column, fields) %}
    {%- for field in fields %}
        JSON_EXTRACT_SCALAR({{ json_column }}, '{{ field.path }}')
            AS {{ field.name }}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
{% endmacro %}
