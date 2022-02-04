-- depends_on: {{ ref('bike_duration') }}
-- depends_on: {{ ref('bike_is_winner') }}

{{ stage_feature_model(
    "bike_is_winner",
    "is_winner",
    "bike_duration",
    ["trip_count_last_week", "trip_duration_last_week"]
) }}
