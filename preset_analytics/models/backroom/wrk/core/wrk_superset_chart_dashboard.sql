{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'}
) }}

SELECT
  dt,
  team_id,
  workspace_id,
  chart_key,
  COUNT(DISTINCT dashboard_key) AS num_dashboards_used_in,
  ARRAY_AGG(COALESCE(dashboard_key, '')) AS dashboard_keys_used_in,
FROM {{ ref('superset_dashboard_chart_history') }}
GROUP BY 1, 2, 3, 4
