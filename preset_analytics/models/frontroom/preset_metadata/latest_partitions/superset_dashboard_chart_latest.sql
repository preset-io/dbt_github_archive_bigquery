SELECT *
FROM {{ ref('src_superset_dashboard_chart') }}
WHERE effective_to IS NULL
