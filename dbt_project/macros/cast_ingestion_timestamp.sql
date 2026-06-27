{%- macro cast_ingestion_timestamp(column, timezone='Europe/Berlin') -%}
cast({{ dbt_date.convert_timezone(column, target_tz=timezone) }} as datetime)
{%- endmacro -%}
