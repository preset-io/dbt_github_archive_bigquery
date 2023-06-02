{{ config(
    alias='wrk_user_persona_attributes_latest',
    materialized='view',
) }}

SELECT
  dt,
  user_id,
  team_id,
  num_explorer_activity,
  num_sql_activity,
  num_dashboard_activity,
  to_date_num_explorer_activity,
  to_date_num_sql_activity,
  to_date_num_dashboard_activity,
  first_explorer_activity_dt,
  latest_explorer_activity_dt,
  first_sql_activity_dt,
  latest_sql_activity_dt,
  first_dashboard_activity_dt,
  latest_dashboard_activity_dt,
  has_explorer_activity,
  has_sql_activity,
  has_dashboard_activity,
FROM {{ ref('wrk_user_persona_attributes') }}
WHERE dt = (SELECT MAX(dt) FROM {{ ref('wrk_user_persona_attributes') }})
