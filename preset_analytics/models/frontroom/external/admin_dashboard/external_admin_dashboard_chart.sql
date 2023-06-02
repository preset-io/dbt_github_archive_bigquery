{{
    config(
        materialized='table',
    )
}}

SELECT
    team_id,
    ds AS dt,
    created_dttm,
    last_modified_dttm,
    chart_name,
    workspace_title,
    viz_type,
    creator_user_id,
    database_engine,
    COALESCE(n7d_views, 0) AS n7d_views,
    COALESCE(n7d_users, 0) AS n7d_users,
    COALESCE(n14d_views, 0) AS n14d_views,
    COALESCE(n14d_users, 0) AS n14d_users,
    COALESCE(n28d_views, 0) AS n28d_views,
    COALESCE(n28d_users, 0) AS n28d_users,
    COALESCE(n90d_views, 0) AS n90d_views,
    COALESCE(n90d_users, 0) AS n90d_users,
    CONCAT('<a href="https://', workspace_hostname, '/superset/explore/?form_data=%7B%22slice_id%22%3A%20', chart_id, '%7D" target="_blank">', chart_name, '</a>') AS chart_link,
FROM {{ ref('superset_chart') }}
WHERE NOT is_example
