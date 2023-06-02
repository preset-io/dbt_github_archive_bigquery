{{ config(materialized='table') }}

SELECT *
FROM {{ ref('src_superset_saved_query') }}
WHERE effective_to IS NULL
