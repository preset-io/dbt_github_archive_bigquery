{{
    config(
        materialized='ephemeral'
    )
}}

SELECT
  ds.dt,
  ws.workspace_id,
  ws.team_id,
  ws.workspace_created_dttm,
  ws.workspace_created_date
FROM {{ ref('date_spine') }} AS ds
LEFT JOIN {{ ref('workspace_spine') }} AS ws
  ON ds.dt >= ws.workspace_created_date
