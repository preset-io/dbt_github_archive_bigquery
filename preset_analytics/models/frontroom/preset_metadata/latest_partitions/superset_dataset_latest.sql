{{ config(materialized='table') }}

SELECT *
FROM {{ ref('src_superset_dataset')}}
WHERE effective_to IS NULL
