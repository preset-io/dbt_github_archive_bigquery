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
    CAST(A.manager_user_id AS STRING) AS user_id,
    B.team_hash,
    A.dttm,
    A.action || COALESCE('_' || A.event_name, '') AS action,
  FROM {{ ref('superset_event_log') }} AS A
  LEFT JOIN {{ ref('manager_team') }} AS B
    ON A.team_id = B.team_id
)

, user_team_agg AS (
  SELECT
    user_id,
    team_hash,
    MIN(dttm) AS first_action,
    MAX(dttm) AS latest_action,
    COUNT(*) AS num_actions,
  FROM all_events
  GROUP BY 1, 2
)

SELECT
  user_id,
  team_hash,
  first_action,
  latest_action,
  num_actions,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY first_action ASC) = 1 AS is_first,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY latest_action DESC) = 1 AS is_latest,
  ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY num_actions DESC) = 1 AS is_most_active,
FROM user_team_agg
