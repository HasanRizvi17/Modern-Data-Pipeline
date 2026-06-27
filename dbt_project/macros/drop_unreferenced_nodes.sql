-- finds staging/intermediate models with no current ref() from any other model, seed, or snapshot, and drops their tables
-- only sees usage inside dbt (ref()/source()) -- a model queried directly by a BI tool would still be flagged
-- usage: dbt run-operation drop_unreferenced_nodes / dbt run-operation drop_unreferenced_nodes --args '{dryrun: false}'

{% macro drop_unreferenced_nodes(dryrun=True) %}
{% if execute %}

    {# 1. collect every node used as a ref()/source() target by some other model, seed, or snapshot #}
    {# tests are deliberately excluded -- almost every model has one, so counting them would hide everything #}
    {% set used_as_parent = [] %}
    {% for node in graph.nodes.values() %}
        {% if node.resource_type in ('model', 'seed', 'snapshot') %}
            {% do used_as_parent.extend(node.depends_on.nodes) %}
        {% endif %}
    {% endfor %}

    {# 2. tally totals and unreferenced models per layer #}
    {% set totals = {'staging': 0, 'intermediate': 0} %}
    {% set unused_counts = {'staging': 0, 'intermediate': 0} %}
    {% set unreferenced = [] %}
    {% for node in graph.nodes.values() %}
        {% set layer = node.path.split('/')[0] %}
        {% if node.resource_type == 'model' and layer in totals %}
            {% do totals.update({layer: totals[layer] + 1}) %}
            {% if node.unique_id not in used_as_parent %}
                {% do unreferenced.append(node) %}
                {% do unused_counts.update({layer: unused_counts[layer] + 1}) %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {# 3. report the counts per layer #}
    {% do log('Staging models: ' ~ unused_counts.staging ~ ' unused out of ' ~ totals.staging ~ ' total', info=True) %}
    {% do log('Intermediate models: ' ~ unused_counts.intermediate ~ ' unused out of ' ~ totals.intermediate ~ ' total', info=True) %}

    {# 4. report the list, and drop only if dryrun is explicitly turned off #}
    {% if unreferenced %}
        {% set names = unreferenced | map(attribute='name') | join(', ') %}
        {% if dryrun %}
            {% do log('Would drop ' ~ unreferenced|length ~ ' unreferenced model(s): ' ~ names, info=True) %}
        {% else %}
            {% for node in unreferenced %}
                {% set relation = api.Relation.create(database=node.database, schema=node.schema, identifier=node.alias) %}
                {% do log('Dropping ' ~ relation, info=True) %}
                {% do adapter.drop_relation(relation) %}
            {% endfor %}
        {% endif %}
    {% else %}
        {% do log('No unreferenced staging/intermediate models found.', info=True) %}
    {% endif %}

{% endif %}
{% endmacro %}
