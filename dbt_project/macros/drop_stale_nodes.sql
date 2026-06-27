-- finds tables/views in our managed datasets that no longer match a current model/seed/snapshot, and drops them
-- usage: dbt run-operation drop_stale_nodes / dbt run-operation drop_stale_nodes --args '{dryrun: false}'

{% macro drop_stale_nodes(dryrun=True) %}
{% if execute %}

    {# 1. group expected table/view names by (database, schema), keyed off the live dbt graph #}
    {% set expected = {} %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type in ('model', 'seed', 'snapshot') and node.config.materialized != 'ephemeral' %}
            {% set key = (node.database, node.schema) %}
            {% do expected.setdefault(key, []).append(node.alias) %}
        {% endif %}
    {% endfor %}

    {# 2. for each dataset, compare what's actually there against what's expected #}
    {% set orphans = [] %}
    {% for key, expected_names in expected.items() %}
        {% set database, schema = key %}
        {% set dataset = api.Relation.create(database=database, schema=schema) %}
        {% for relation in adapter.list_relations_without_caching(dataset) %}
            {% if relation.identifier not in expected_names %}
                {% do orphans.append(relation) %}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {# 3. sort by database, then schema, then table name, so output is grouped dataset by dataset #}
    {% set orphans = orphans | sort(attribute='database,schema,identifier') %}

    {# 4. report, and drop only if dryrun is explicitly turned off #}
    {% if orphans %}
        {% if dryrun %}
            {% do log('Would drop ' ~ orphans|length ~ ' orphaned relation(s):', info=True) %}
            {% for relation in orphans %}
                {% do log('  - ' ~ relation, info=True) %}
            {% endfor %}
        {% else %}
            {% for relation in orphans %}
                {% do log('Dropping ' ~ relation, info=True) %}
                {% do adapter.drop_relation(relation) %}
            {% endfor %}
        {% endif %}
    {% else %}
        {% do log('No orphaned relations found.', info=True) %}
    {% endif %}

{% endif %}
{% endmacro %}
