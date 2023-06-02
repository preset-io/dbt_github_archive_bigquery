WITH manager_team_membership_latest AS (
    {{ latest_partition(ref('src_manager_team_membership')) }}
)

SELECT *
FROM manager_team_membership_latest
