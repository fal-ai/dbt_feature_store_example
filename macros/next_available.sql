{% macro next_available(
        feature_table,
        entity_column,
        timestamp_column,
        feature_columns=['*']
    ) %}
SELECT
    {% for column in feature_columns %}
        {{ column }},
    {% endfor %}

    cast({{ entity_column }} AS string) AS __f__entity,
    cast({{ timestamp_column }} AS timestamp) AS __f__timestamp,
    {{ next_timestamp(entity_column, timestamp_column) }} AS __f__next_timestamp
FROM {{ feature_table }}
{% endmacro %}
