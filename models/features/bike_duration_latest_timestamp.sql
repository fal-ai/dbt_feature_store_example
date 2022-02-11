{{ feature_store.latest_timestamp(
  {
    'table': ref('bike_duration'),
    'columns': ['trip_duration_last_week', 'trip_count_last_week']
  }
) }}
