WITH manager_preset_user_latest AS (
    {{ latest_partition(ref('src_manager_preset_user')) }}
)

SELECT *
FROM manager_preset_user_latest
