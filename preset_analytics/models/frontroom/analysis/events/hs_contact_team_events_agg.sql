WITH all_events AS (
  SELECT
    C.hs_contact_id,
    B.team_hash,
    A.dttm,
    A.action || COALESCE('_' || A.event_name, '') AS action,
  FROM {{ ref('superset_event_log') }} AS A
  LEFT JOIN {{ ref('manager_team') }} AS B
    ON A.team_id = B.team_id
  LEFT JOIN {{ ref('manager_user') }} AS C
    ON A.manager_user_id = C.user_id
  WHERE C.hs_contact_id IS NOT NULL
)

, user_team_agg AS (
  SELECT
    hs_contact_id,
    team_hash,
    MIN(dttm) AS first_action,
    MAX(dttm) AS latest_action,
    COUNT(*) AS num_actions,
  FROM all_events
  GROUP BY 1, 2
)

SELECT
  hs_contact_id,
  team_hash,
  first_action,
  latest_action,
  num_actions,
  ROW_NUMBER() OVER (PARTITION BY hs_contact_id ORDER BY first_action ASC) = 1 AS is_first,
  ROW_NUMBER() OVER (PARTITION BY hs_contact_id ORDER BY latest_action DESC) = 1 AS is_latest,
  ROW_NUMBER() OVER (PARTITION BY hs_contact_id ORDER BY num_actions DESC) = 1 AS is_most_active,
FROM user_team_agg
