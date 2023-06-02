{{
    config(
        materialized='table',
    )
}}

SELECT
    team_id,
    ds AS dt,
    workspace_title,
    COALESCE(n7d_users, 0) AS n7d_users,
    COALESCE(n7d_views, 0) AS n7d_views,
    COALESCE(n28d_users, 0) AS n28d_users,
    COALESCE(n28d_views, 0) AS n28d_views,
    COALESCE(ltd_users, 0) AS ltd_users,
    COALESCE(ltd_views, 0) AS ltd_views,
    CONCAT('<a href="https://', workspace_hostname, '/superset/dashboard/', dashboard_id, '/" target="_blank">', dashboard_title, '</a>') AS dashboard_link,
    creator_user_id
FROM {{ ref('superset_dashboard') }}
WHERE NOT is_example
