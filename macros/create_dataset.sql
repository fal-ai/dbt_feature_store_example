{% macro create_dataset(
        features,
        label_table_model,
        label_name
    ) %}
{% set feature_columns = ["trip_count_last_week", "trip_duration_last_week"] %}
{% set label_table = namespace(
    timestamp_column = "",
    entity_id_column = ""
) %}
{% if execute %}

{% set node = graph.nodes["model.bike." + label_table_model] %}
{% set label_table.timestamp_column = node.config.meta.fal.feature_store.timestamp %}
{% set label_table.entity_id_column = node.config.meta.fal.feature_store.entity_id %}

SELECT
    {% for column in feature_columns %}
    {{ column }},
    {% endfor %}
    lb.{{ label_name }},
    lb.{{ label_table.entity_id_column }},
    lb.{{ label_table.timestamp_column }}
FROM
    {{ ref(label_table_model) }} AS lb

{% for group in features %}
{{ stage_table_join(
    "stg_" + group.feature_model_name + "_" + label_table_model,
    "bike_id",
    "date",
    "lb",
    label_table.entity_id_column,
    label_table.timestamp_column
) }}
{% endfor %}

{% endif %}
{% endmacro %}

{% macro stage_table_join(
        stage_table,
        stage_table_entity_id_column,
        stage_table_timestamp_column,
        label_table,
        label_table_entity_id,
        label_table_timestamp
    ) %}
INNER JOIN {{ ref(stage_table) }}
    ON {{ label_table }}.{{ label_table_entity_id }} = {{ stage_table }}.{{ stage_table_entity_id_column }}
    AND {{ label_table }}.{{ label_table_timestamp }} = {{ stage_table }}.{{ stage_table_timestamp_column }}
{% endmacro %}
