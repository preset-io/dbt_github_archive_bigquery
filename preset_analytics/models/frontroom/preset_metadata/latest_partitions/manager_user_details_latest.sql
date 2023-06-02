WITH manager_user_details_latest AS (
    {{ latest_partition(ref('src_manager_user_details')) }}
)

SELECT *
FROM manager_user_details_latest
