{{ config(materialized='table') }}

SELECT
  *
FROM {{ ref('superset_database_connection_history') }}
WHERE dt = (SELECT MAX(dt) FROM {{ ref('superset_database_connection_history') }})
