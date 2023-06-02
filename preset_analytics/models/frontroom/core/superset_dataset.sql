{{ config(materialized='table') }}

SELECT
  *
FROM {{ ref('superset_dataset_history') }}
WHERE dt = (SELECT MAX(dt) FROM {{ref('superset_dataset_history')}})
