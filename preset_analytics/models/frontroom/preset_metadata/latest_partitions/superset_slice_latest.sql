{{ config(materialized='table') }}

SELECT *
FROM {{ ref('src_superset_slice') }}
WHERE effective_to IS NULL
