WITH hs_contact_team AS (
    SELECT DISTINCT
        contact.hs_contact_id,
        team_event.workspace_hostname,
        team_event.first_action,
        team_event.latest_action,
        team_event.num_actions,
    FROM {{ ref('manager_user') }} AS contact
    LEFT JOIN {{ ref('manager_team_membership_latest') }} AS team
      ON contact.user_id = team.user_id
    LEFT JOIN {{ ref('team_workspace_events_agg') }} AS team_event
      ON team.team_id = team_event.team_id
    WHERE contact.hs_contact_id IS NOT NULL
      AND team.team_id IS NOT NULL
      AND (team_event.is_first OR team_event.is_latest OR team_event.is_most_active) -- only grab workspaces of interest
)

SELECT
  hs_contact_id,
  workspace_hostname,
  first_action,
  latest_action,
  num_actions,
  ROW_NUMBER() OVER (PARTITION BY hs_contact_id ORDER BY first_action ASC) = 1 AS is_first,
  ROW_NUMBER() OVER (PARTITION BY hs_contact_id ORDER BY latest_action DESC) = 1 AS is_latest,
  ROW_NUMBER() OVER (PARTITION BY hs_contact_id ORDER BY num_actions DESC) = 1 AS is_most_active,
FROM hs_contact_team
