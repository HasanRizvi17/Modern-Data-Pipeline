-- macro to convert amounts from USD, CAD and GBP to EUR

{%- macro convert_to_euro(amount, rate) -%}
CASE
    WHEN {{ rate }} IS NULL THEN {{ amount }}
    ELSE {{ amount }} * {{ rate }}
END
{%- endmacro -%}
