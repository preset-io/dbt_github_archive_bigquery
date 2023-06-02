{{ config(
    materialized='table',
    tags=['egaf'],
) }}
SELECT
  A.*,
  A.entity_type AS entity,  -- for backwards compatiblity
  A.daily_active AS daily_active_entity,
  A.weekly_active AS weekly_active_entity,
  A.monthly_active AS monthly_active_entity,
  A.quarterly_active AS quarterly_active_entity,
  {{ team_attributes(alias="B", include_team_id=False) }}
FROM {{ ref('wrk_egaf_summary') }} AS A
LEFT JOIN {{ ref('manager_team_history') }} AS B
  ON A.team_id = B.team_id
    AND A.dt = B.ds
-- records aggregated at the full team level
WHERE A.workspace_hash = 'ALL WORKSPACES'
