WITH all_events AS (
  SELECT
    B.hs_contact_id,
    A.workspace_hostname,
    A.dttm,
    A.action || COALESCE('_' || A.event_name, '') AS action,
  FROM {{ ref('superset_event_log') }} AS A
  LEFT JOIN {{ ref('manager_user') }} AS B
    ON A.manager_user_id = B.user_id
  WHERE B.hs_contact_id IS NOT NULL
)

, user_team_agg AS (
  SELECT
    hs_contact_id,
    workspace_hostname,
    MIN(dttm) AS first_action,
    MAX(dttm) AS latest_action,
    COUNT(*) AS num_actions,
  FROM all_events
  WHERE workspace_hostname IS NOT NULL
  GROUP BY 1, 2
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
FROM user_team_agg
