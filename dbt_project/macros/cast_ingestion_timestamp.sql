{%- macro cast_ingestion_timestamp(column, timezone='Europe/Berlin') -%}
DATETIME(TIMESTAMP({{ column }}), "{{ timezone }}")
{%- endmacro -%}
