WITH hs_contact AS (
    SELECT DISTINCT
        hs_contact_id
    FROM {{ ref('manager_user') }}
    WHERE hs_contact_id IS NOT NULL
)

SELECT
    hs_contact.hs_contact_id,
    team_agg_first.team_hash AS team_hash_first,
    team_agg_latest.team_hash AS team_hash_latest,
    team_agg_most_active.team_hash AS team_hash_most_active,
    COALESCE(workspace_agg_first.workspace_hostname, team_workspace_agg_first.workspace_hostname) AS workspace_hostname_first,
    COALESCE(workspace_agg_latest.workspace_hostname, team_workspace_agg_latest.workspace_hostname) AS workspace_hostname_latest,
    COALESCE(workspace_agg_most_active.workspace_hostname, team_workspace_agg_most_active.workspace_hostname) AS workspace_hostname_is_most_active,
FROM hs_contact
LEFT JOIN {{ ref('hs_contact_team_events_agg') }} AS team_agg_first
    ON hs_contact.hs_contact_id = team_agg_first.hs_contact_id
        AND team_agg_first.is_first IS true
LEFT JOIN {{ ref('hs_contact_team_events_agg') }} AS team_agg_latest
    ON hs_contact.hs_contact_id = team_agg_latest.hs_contact_id
        AND team_agg_latest.is_latest IS true
LEFT JOIN {{ ref('hs_contact_team_events_agg') }} AS team_agg_most_active
    ON hs_contact.hs_contact_id = team_agg_most_active.hs_contact_id
        AND team_agg_most_active.is_most_active IS true
LEFT JOIN {{ ref('hs_contact_workspace_events_agg') }} AS workspace_agg_first
    ON hs_contact.hs_contact_id = workspace_agg_first.hs_contact_id
        AND workspace_agg_first.is_first IS true
LEFT JOIN {{ ref('hs_contact_workspace_events_agg') }} AS workspace_agg_latest
    ON hs_contact.hs_contact_id = workspace_agg_latest.hs_contact_id
        AND workspace_agg_latest.is_latest IS true
LEFT JOIN {{ ref('hs_contact_workspace_events_agg') }} AS workspace_agg_most_active
    ON hs_contact.hs_contact_id = workspace_agg_most_active.hs_contact_id
        AND workspace_agg_most_active.is_most_active IS true
LEFT JOIN {{ ref('hs_contact_team_workspace_events_agg') }} AS team_workspace_agg_first
    ON hs_contact.hs_contact_id = team_workspace_agg_first.hs_contact_id
        AND team_workspace_agg_first.is_first IS true
LEFT JOIN {{ ref('hs_contact_team_workspace_events_agg') }} AS team_workspace_agg_latest
    ON hs_contact.hs_contact_id = team_workspace_agg_latest.hs_contact_id
        AND team_workspace_agg_latest.is_latest IS true
LEFT JOIN {{ ref('hs_contact_team_workspace_events_agg') }} AS team_workspace_agg_most_active
    ON hs_contact.hs_contact_id = team_workspace_agg_most_active.hs_contact_id
        AND team_workspace_agg_most_active.is_most_active IS true
