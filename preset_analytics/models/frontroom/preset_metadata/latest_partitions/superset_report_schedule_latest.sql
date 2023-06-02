{{ config(materialized='table') }}

SELECT *
FROM {{ ref('src_superset_report_schedule') }}
WHERE effective_to IS NULL
