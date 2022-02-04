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

        -- TODO: remove casting?
        cast({{ feature_entity_column }} AS string) AS __f__entity,
        cast({{ feature_timestamp_column }} AS timestamp) AS __f__timestamp,
        cast({{ next_timestamp(feature_entity_column, feature_timestamp_column) }} AS timestamp) AS __f__next_timestamp
    FROM {{ feature_table }}
), __f__label AS (
    SELECT
        {{ label_entity_column }},
        {{ label_timestamp_column }},
        {{ label_column }},

        -- TODO: remove casting?
        cast({{ label_entity_column }} AS string) AS __f__entity,
        cast({{ label_timestamp_column }} AS timestamp) AS __f__timestamp
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
{% set ns = namespace(
    feature_entity_id_column = "",
    feature_timestamp_column = "",
    label_entity_id_column = "",
    label_timestamp_column = ""
) %}
{% if execute %}

-- TODO: find node with just model name
{% set node = graph.nodes["model.bike." + feature_table_model] %}
{% set ns.feature_entity_id_column = node.config.meta.fal.feature_store.entity_id %}
{% set ns.feature_timestamp_column = node.config.meta.fal.feature_store.timestamp %}

{% set node = graph.nodes["model.bike." + label_table_model] %}
{% set ns.label_entity_id_column = node.config.meta.fal.feature_store.entity_id %}
{% set ns.label_timestamp_column = node.config.meta.fal.feature_store.timestamp %}

{{ stage_feature_table(
    ref(label_table_model),
    ns.label_entity_id_column,
    ns.label_timestamp_column,
    label_column,
    ref(feature_table_model),
    ns.feature_entity_id_column,
    ns.feature_timestamp_column,
    feature_columns
) }}

{% endif %}
{% endmacro %}
