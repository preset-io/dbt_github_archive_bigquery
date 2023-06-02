{{ config(materialized='table') }}

SELECT
    CAST(dttm AS DATE) AS dt,
    CASE
        WHEN action = 'dashboard' THEN dashboard_key
        WHEN action = 'explore' THEN chart_key
        WHEN action = 'log' AND event_name IN ('mount_dashboard', 'periodic_render_dashboard', 'force_refresh_dashboard') AND dashboard_key IS NOT NULL THEN dashboard_key
        WHEN action = 'log' AND event_name IN ('mount_explorer', 'explore_dashboard_chart', 'force_refresh_chart') AND chart_key IS NOT NULL THEN chart_key
    END AS object_key,
    manager_user_id,
    team_id,
    action,
    CASE WHEN action = 'log' AND event_name IN ('mount_dashboard', 'periodic_render_dashboard', 'force_refresh_dashboard') AND dashboard_key IS NOT NULL THEN 'dashboard'
         WHEN action = 'log' AND event_name IN ('mount_explorer', 'explore_dashboard_chart', 'force_refresh_chart') AND chart_key IS NOT NULL THEN 'chart'
         END as object_type,
    COUNT(*) AS views,
FROM {{ ref('wrk_superset_events') }}
WHERE action IN ('dashboard', 'explore', 'log')
GROUP BY 1, 2, 3, 4, 5, 6
