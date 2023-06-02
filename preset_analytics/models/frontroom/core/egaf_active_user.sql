{{ config(
    materialized='table',
    tags=['egaf'],
) }}

SELECT
    A.dt,
    A.monthly_active_entity AS mau,
    A.weekly_active_entity AS wau,
    A.daily_active_entity AS dau,
    A.quarterly_active_entity AS qau,
    {{ team_attributes(alias="B", include_core=False) }}
FROM {{ ref('egaf_active_entity') }} AS A
LEFT JOIN {{ ref('manager_team_history') }} AS B
  ON A.team_id = B.team_id
    AND A.dt = B.ds
WHERE A.entity_type = 'user'
