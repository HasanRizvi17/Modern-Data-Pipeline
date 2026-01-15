{% macro cast_iso_datetimes(columns, timezone='Europe/Berlin') %}
    {%- for col in columns %}
        DATETIME(
            TIMESTAMP(
                SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', {{ col }})
            ),
            "{{ timezone }}"
        ) AS {{ col }}
        {%- if not loop.last %},{% endif %}
    {%- endfor %}
{% endmacro %}
