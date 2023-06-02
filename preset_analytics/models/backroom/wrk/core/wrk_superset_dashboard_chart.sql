{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'}
) }}

SELECT
  dt,
  team_id,
  workspace_id,
  dashboard_key,
  COUNT(DISTINCT chart_key) AS num_charts,
  ARRAY_AGG(COALESCE(chart_key, '')) AS chart_keys_in_dashboard,
FROM {{ ref('superset_dashboard_chart_history') }}
GROUP BY 1, 2, 3, 4
