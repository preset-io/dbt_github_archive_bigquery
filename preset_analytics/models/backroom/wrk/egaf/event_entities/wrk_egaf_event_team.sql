SELECT
  'team' AS entity_type,
  dt,
  -1 AS team_id,
  'ALL WORKSPACES' AS workspace_hash,
  CAST(team_id AS STRING) AS entity_id,
  False AS is_example,
  event_id,
FROM {{ ref('_wrk_egaf_event_base') }}
WHERE team_id NOT IN (
  SELECT team_id
  FROM {{ ref('wrk_manager_team_latest') }}
  WHERE is_preset
    OR is_duplicate_team
)
