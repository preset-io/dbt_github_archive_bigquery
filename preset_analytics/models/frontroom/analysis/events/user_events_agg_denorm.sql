WITH user AS (
    SELECT DISTINCT
        CAST(user_id AS STRING) AS user_id
    FROM {{ ref('manager_user') }}
    WHERE user_id IS NOT NULL
)

SELECT
    user.user_id,
    team_agg_first.team_hash AS team_hash_first,
    team_agg_latest.team_hash AS team_hash_latest,
    team_agg_most_active.team_hash AS team_hash_most_active,
    workspace_agg_first.workspace_hostname AS workspace_hostname_first,
    workspace_agg_latest.workspace_hostname AS workspace_hostname_latest,
    workspace_agg_most_active.workspace_hostname AS workspace_hostname_is_most_active,
FROM user
LEFT JOIN {{ ref('user_team_events_agg') }} AS team_agg_first
    ON user.user_id = team_agg_first.user_id
        AND team_agg_first.is_first IS true
LEFT JOIN {{ ref('user_team_events_agg') }} AS team_agg_latest
    ON user.user_id = team_agg_latest.user_id
        AND team_agg_latest.is_latest IS true
LEFT JOIN {{ ref('user_team_events_agg') }} AS team_agg_most_active
    ON user.user_id = team_agg_most_active.user_id
        AND team_agg_most_active.is_most_active IS true
LEFT JOIN {{ ref('user_workspace_events_agg') }} AS workspace_agg_first
    ON user.user_id = workspace_agg_first.user_id
        AND workspace_agg_first.is_first IS true
LEFT JOIN {{ ref('user_workspace_events_agg') }} AS workspace_agg_latest
    ON user.user_id = workspace_agg_latest.user_id
        AND workspace_agg_latest.is_latest IS true
LEFT JOIN {{ ref('user_workspace_events_agg') }} AS workspace_agg_most_active
    ON user.user_id = workspace_agg_most_active.user_id
        AND workspace_agg_most_active.is_most_active IS true
