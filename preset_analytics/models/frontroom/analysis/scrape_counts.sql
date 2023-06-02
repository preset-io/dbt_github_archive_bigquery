{{ config(materialized='table') }}
{% set tbls = [
    "superset_dashboard",
    "superset_slice",
    "superset_dataset",
    "superset_database_connection",
    "superset_report_schedule",
    "superset_saved_query"
]%}

{% for tbl in tbls %}
SELECT ds, '{{ tbl }}' AS table_name, COUNT(1) AS row_count, COUNT(DISTINCT workspace_id) AS distinct_workspaces
FROM `preset-cloud-analytics`.`production_preset_metadata`.{{ tbl }}
WHERE ds >= DATE('1970-01-01')
GROUP BY 1, 2
UNION ALL
{% endfor %}
SELECT ds, 'manager_workspace', COUNT(1) AS row_count, COUNT(DISTINCT id) AS distinct_workspaces
FROM `preset-cloud-analytics`.`production_preset_metadata`.manager_workspace
WHERE ds >= DATE('1970-01-01')
GROUP BY 1, 2
