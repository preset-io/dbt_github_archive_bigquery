{{
    config(
        materialized='ephemeral'
    )
}}

SELECT
  id AS workspace_id,
  team_id,
  MIN(created_dttm) AS workspace_created_dttm,
  MIN(DATE(created_dttm)) AS workspace_created_date
FROM {{ ref('src_manager_workspace') }}
GROUP BY 1, 2
