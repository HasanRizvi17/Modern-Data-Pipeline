-- macro to apply discount based on type (fixed or percentage)
-- discount_amount for 'percentage' is stored as a whole percent (e.g. 10 = 10%), hence the /100

{%- macro apply_discount(discount_type, discount_amount, gross_value) -%}
CASE
    WHEN {{ discount_type }} = 'fixed' THEN {{ discount_amount }}
    WHEN {{ discount_type }} = 'percentage' THEN ({{ discount_amount }} / 100) * {{ gross_value }}
    ELSE 0
END
{%- endmacro -%}
