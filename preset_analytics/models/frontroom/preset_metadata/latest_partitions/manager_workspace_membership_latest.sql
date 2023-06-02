WITH manager_workspace_membership_latest AS (
    {{ latest_partition(ref('src_manager_workspace_membership')) }}
)

SELECT * FROM manager_workspace_membership_latest
