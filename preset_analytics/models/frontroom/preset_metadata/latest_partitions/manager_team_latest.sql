WITH manager_team_latest AS (
    {{ latest_partition(ref('src_manager_team')) }}
)

SELECT *
FROM manager_team_latest
