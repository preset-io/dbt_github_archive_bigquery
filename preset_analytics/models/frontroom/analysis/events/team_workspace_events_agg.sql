WITH all_events AS (
  -- TODO: verify manager entries with null for team_id

  -- SELECT
  --   CAST(A.user_id AS STRING) AS user_id,
  --   team_id,
  --   A.dttm,
  --   A.action,
  -- FROM core.manager_event_log
  -- LEFT JOIN core.manager_team AS B
  --   ON A.team_id = B.team_id

  -- UNION ALL

  SELECT
    team_id,
    workspace_hostname,
    dttm,
    action || COALESCE('_' || event_name, '') AS action,
  FROM {{ ref('superset_event_log') }}
  WHERE workspace_hostname IS NOT NULL
)

, user_team_agg AS (
  SELECT
    team_id,
    workspace_hostname,
    MIN(dttm) AS first_action,
    MAX(dttm) AS latest_action,
    COUNT(*) AS num_actions,
  FROM all_events
  GROUP BY 1, 2
)

SELECT
  team_id,
  workspace_hostname,
  first_action,
  latest_action,
  num_actions,
  ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY first_action ASC) = 1 AS is_first,
  ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY latest_action DESC) = 1 AS is_latest,
  ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY num_actions DESC) = 1 AS is_most_active,
FROM user_team_agg
