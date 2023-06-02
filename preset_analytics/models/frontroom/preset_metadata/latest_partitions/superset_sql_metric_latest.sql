{{ config(materialized='table') }}

SELECT *
FROM {{ ref('src_superset_sql_metric')}}
WHERE effective_to IS NULL
