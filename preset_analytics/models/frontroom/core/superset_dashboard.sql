{{ config(materialized='table') }}

SELECT
  *
FROM {{ ref('superset_dashboard_history') }}
WHERE dt = (SELECT MAX(dt) FROM {{ ref('superset_dashboard_history') }})
