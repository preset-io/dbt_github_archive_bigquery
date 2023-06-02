WITH manager_invite_latest AS (
    {{ latest_partition(ref('src_manager_invite')) }}
)

SELECT *
FROM manager_invite_latest
