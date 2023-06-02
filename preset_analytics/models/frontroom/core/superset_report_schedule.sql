{{ config(materialized='table') }}

SELECT
  *
FROM {{ ref('superset_report_schedule_history') }}
WHERE dt = (SELECT MAX(dt) FROM {{ref('superset_report_schedule_history')}})
