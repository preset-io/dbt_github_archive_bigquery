SELECT
    CONCAT(user_id, '_', team_id) AS pk,
    CAST(user_id AS STRING) AS user_id,
    CAST(team_id AS STRING) AS team_id,
FROM {{ ref('manager_team_membership_latest') }}
