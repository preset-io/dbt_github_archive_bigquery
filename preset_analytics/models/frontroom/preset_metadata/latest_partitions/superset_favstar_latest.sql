{{ config(materialized='table') }}

SELECT *
FROM {{ ref('src_superset_favstar')}}
WHERE effective_to IS NULL
