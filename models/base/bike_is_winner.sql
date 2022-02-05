SELECT
    bike_id,
    cast(date AS timestamp) AS date,
    is_winner
FROM
    {{ source(
        "dbt_meder_bike",
        "bike_is_winner"
    ) }}
