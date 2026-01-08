-- macro to apply discount based on type (fixed or percentage)

{%- macro apply_discount(discount_type, discount_amount, gross_value) -%}
CASE
    WHEN {{ discount_type }} = 'fixed' THEN {{ discount_amount }}
    WHEN {{ discount_type }} = 'percentage' THEN {{ discount_amount }} * {{ gross_value }}
    ELSE 0
END
{%- endmacro -%}
