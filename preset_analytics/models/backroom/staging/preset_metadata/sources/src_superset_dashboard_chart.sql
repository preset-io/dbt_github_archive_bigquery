SELECT
    ds,
    team_id,
    workspace_id,
    CONCAT(workspace_id, '_', dashboard_id) AS dashboard_key,
    CONCAT(workspace_id, '_', slice_id) AS chart_key,
    dashboard_id,
    slice_id AS chart_id,
    ds AS effective_from,
    LEAD(ds) OVER (PARTITION BY team_id, workspace_id, dashboard_id, slice_id ORDER BY ds) AS effective_to,
FROM {{ ref('src_superset_dashboard_chart_dedup') }}
WHERE ds >= {{ var("start_date") }}
    AND dashboard_id IS NOT NULL
    AND slice_id IS NOT NULL
