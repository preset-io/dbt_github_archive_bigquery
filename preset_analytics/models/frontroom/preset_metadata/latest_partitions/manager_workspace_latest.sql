WITH manager_workspace_latest AS (
    {{ latest_partition(ref('src_manager_workspace')) }}
)

SELECT *
FROM manager_workspace_latest
