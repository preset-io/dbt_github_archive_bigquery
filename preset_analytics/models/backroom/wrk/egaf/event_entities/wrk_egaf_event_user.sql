-- Users (any action)
SELECT
  'user' AS entity_type,
  dt,
  team_id,
  workspace_hash,
  CAST(manager_user_id AS STRING) AS entity_id,
  False AS is_example,
  event_id,
FROM {{ ref('_wrk_egaf_event_base') }}
WHERE manager_user_id IS NOT NULL
