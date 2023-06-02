{{
    config(
        materialized='ephemeral'
    )
}}

SELECT
  ds.dt,
  ts.team_id,
  ts.team_created_dttm
FROM {{ ref('date_spine') }} AS ds
LEFT JOIN {{ ref('team_spine') }} AS ts
  ON ds.dt >= ts.team_created_dttm
