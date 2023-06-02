WITH team AS (
    SELECT DISTINCT
        team_id
    FROM {{ ref('manager_team') }}
    WHERE team_id IS NOT NULL
)

SELECT
    team.team_id,
    workspace_agg_first.workspace_hostname AS workspace_hostname_first,
    workspace_agg_latest.workspace_hostname AS workspace_hostname_latest,
    workspace_agg_most_active.workspace_hostname AS workspace_hostname_is_most_active,
FROM team
LEFT JOIN {{ ref('team_workspace_events_agg') }} AS workspace_agg_first
    ON team.team_id = workspace_agg_first.team_id
        AND workspace_agg_first.is_first IS true
LEFT JOIN {{ ref('team_workspace_events_agg') }} AS workspace_agg_latest
    ON team.team_id = workspace_agg_latest.team_id
        AND workspace_agg_latest.is_latest IS true
LEFT JOIN {{ ref('team_workspace_events_agg') }} AS workspace_agg_most_active
    ON team.team_id = workspace_agg_most_active.team_id
        AND workspace_agg_most_active.is_most_active IS true
