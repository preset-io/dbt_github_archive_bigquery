{{ config(materialized='table') }}

SELECT *
FROM {{ ref('src_superset_dashboard') }}
WHERE effective_to IS NULL
