{{ config(materialized='table') }}

SELECT *
FROM {{ ref('src_superset_embedded_dashboard') }}
WHERE effective_to IS NULL
