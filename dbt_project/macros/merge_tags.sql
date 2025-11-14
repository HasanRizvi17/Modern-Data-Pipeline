{% macro merge_tags(extra_tags) %}
    {# Get default tags from project config #}
    {% set default_tags = config.get('tags', []) %}
    {% set merged = default_tags + extra_tags %}
    {{ return(merged | unique) }}
{% endmacro %}
