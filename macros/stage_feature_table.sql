{% macro stage_feature_table(
        label_table,
        label_entity_column,
        label_timestamp_column,
        label_column,
        feature_table,
        feature_entity_column,
        feature_timestamp_column,
        feature_columns=['*']
    ) %}
WITH __f__most_recent AS (
    SELECT
        {% for column in feature_columns %}
        {{ column }},
        {% endfor %}

        {{ feature_entity_column }} AS __f__entity,
        {{ feature_timestamp_column }} AS __f__timestamp,
        {{ next_timestamp(feature_entity_column, feature_timestamp_column) }} AS __f__next_timestamp
    FROM {{ feature_table }}
), __f__label AS (
    SELECT
        {{ label_entity_column }},
        {{ label_timestamp_column }},
        {{ label_column }},

        {{ label_entity_column }} AS __f__entity,
        {{ label_timestamp_column }} AS __f__timestamp
    FROM {{ label_table }}
)

SELECT
    {% for column in feature_columns %}
    {{ column }},
    {% endfor %}

    lb.{{ label_entity_column }},
    lb.{{ label_timestamp_column }},

    lb.{{ label_column }}
FROM __f__label AS lb
LEFT JOIN __f__most_recent AS mr
    ON lb.__f__entity = mr.__f__entity
    AND mr.__f__timestamp < lb.__f__timestamp
    AND (
        mr.__f__next_timestamp IS NULL
        OR lb.__f__timestamp <= mr.__f__next_timestamp
    )
{% endmacro %}

{% macro stage_feature_model(
        label_table_model,
        label_column,
        feature_table_model,
        feature_columns=['*']
    ) %}
{% set feature_table = namespace(
    entity_id_column = "",
    timestamp_column = "",
) %}
{% set label_table = namespace(
    entity_id_column = "",
    timestamp_column = ""
) %}
{% if execute %}

-- TODO: find node with just model name
{% set node = graph.nodes["model.bike." + feature_table_model] %}
{% set feature_table.entity_id_column = node.config.meta.fal.feature_store.entity_id %}
{% set feature_table.timestamp_column = node.config.meta.fal.feature_store.timestamp %}

{% set node = graph.nodes["model.bike." + label_table_model] %}
{% set label_table.entity_id_column = node.config.meta.fal.feature_store.entity_id %}
{% set label_table.timestamp_column = node.config.meta.fal.feature_store.timestamp %}

{{ stage_feature_table(
    ref(label_table_model),
    label_table.entity_id_column,
    label_table.timestamp_column,
    label_column,
    ref(feature_table_model),
    feature_table.entity_id_column,
    feature_table.timestamp_column,
    feature_columns
) }}

{% endif %}
{% endmacro %}
