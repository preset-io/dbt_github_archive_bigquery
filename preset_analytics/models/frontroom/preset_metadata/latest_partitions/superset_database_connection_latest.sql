{{ config(materialized='table') }}

SELECT *
FROM {{ ref('src_superset_database_connection') }}
WHERE effective_to IS NULL
