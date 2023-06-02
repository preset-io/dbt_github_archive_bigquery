-- Dashboards, to count the number of ACTIVE (consumed) dashboards
SELECT
  'dashboard' AS entity_type,
  A.dt,
  A.team_id,
  A.workspace_hash,
  A.dashboard_key AS entity_id,
  B.is_example AS is_example,
  A.event_id,
FROM {{ ref('_wrk_egaf_event_base') }} AS A
INNER JOIN {{ ref('superset_dashboard_latest') }} AS B
  ON A.dashboard_key = B.dashboard_key
WHERE A.dashboard_key IS NOT NULL
