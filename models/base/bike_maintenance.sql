SELECT
    bike_id,
    timestamp
FROM
    {{ source(
        "samples",
        "bike_dataset_labels_maintenance_required"
    ) }}
