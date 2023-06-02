{{ config(materialized='table') }}

SELECT
  *
FROM {{ ref('superset_saved_query_history') }}
WHERE dt = (SELECT MAX(dt) FROM {{ref('superset_saved_query_history')}})
