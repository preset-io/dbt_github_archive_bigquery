{{ config(materialized='table') }}

SELECT
  *
FROM {{ ref('manager_workspace_history') }}
WHERE ds = (SELECT MAX(ds) FROM {{ref('manager_workspace_history')}})
