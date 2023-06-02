{{
    config(
        materialized='table'
    )
}}

WITH engagement_growth_accounting_framework AS (
    -- base filters on the egaf dataset
    SELECT * REPLACE (COALESCE(workspace_hash, 'ALL WORKSPACES') AS workspace_hash)
    FROM {{ ref('wrk_egaf_summary') }}
    WHERE NOT is_example
        AND team_id IS NOT NULL
),

users AS (
    SELECT
        dt,
        team_id,
        workspace_hash,
        -- l7, -- sum(entitiy_count)
        -- l28, -- sum(entitiy_count)
        SUM(daily_active) AS daily_active_users,
        SUM(weekly_active) AS weekly_active_users,
        SUM(monthly_active) AS monthly_active_users,
    FROM engagement_growth_accounting_framework
    WHERE entity_type = 'user'
    GROUP BY 1, 2, 3
),

dashboards AS (
    SELECT
        dt,
        team_id,
        workspace_hash,
        -- l7, -- sum(entitiy_count)
        -- l28, -- sum(entitiy_count)
        SUM(daily_active) AS daily_active_dashboards,
        SUM(weekly_active) AS weekly_active_dashboards,
        SUM(monthly_active) AS monthly_active_dashboards,
    FROM engagement_growth_accounting_framework
    WHERE entity_type = 'dashboard'
    GROUP BY 1, 2, 3
),

charts AS (
    SELECT
        dt,
        team_id,
        workspace_hash,
        -- l7, -- sum(entitiy_count)
        -- l28, -- sum(entitiy_count)
        SUM(daily_active) AS daily_active_charts,
        SUM(weekly_active) AS weekly_active_charts,
        SUM(monthly_active) AS monthly_active_charts,
    FROM engagement_growth_accounting_framework
    WHERE entity_type = 'chart'
    GROUP BY 1, 2, 3
)

SELECT
    users.dt,
    users.team_id,
    users.workspace_hash,
    COALESCE(workspace.workspace_title, users.workspace_hash) AS workspace_title,

    -- l7, -- sum(entitiy_count)
    -- l28, -- sum(entitiy_count)

    COALESCE(users.daily_active_users, 0) AS daily_active_users,
    COALESCE(users.weekly_active_users, 0) AS weekly_active_users,
    COALESCE(users.monthly_active_users, 0) AS monthly_active_users,
    SAFE_DIVIDE(COALESCE(users.weekly_active_users, 0), users.monthly_active_users) AS weekly_to_monthly_active_users,

    COALESCE(dashboards.daily_active_dashboards, 0) AS daily_active_dashboards,
    COALESCE(dashboards.weekly_active_dashboards, 0) AS weekly_active_dashboards,
    COALESCE(dashboards.monthly_active_dashboards, 0) AS monthly_active_dashboards,

    COALESCE(charts.daily_active_charts, 0) AS daily_active_charts,
    COALESCE(charts.weekly_active_charts, 0) AS weekly_active_charts,
    COALESCE(charts.monthly_active_charts, 0) AS monthly_active_charts,

FROM users
LEFT JOIN dashboards
    ON users.dt = dashboards.dt
    AND users.team_id = dashboards.team_id
    AND users.workspace_hash = dashboards.workspace_hash
LEFT JOIN charts
    ON users.dt = charts.dt
    AND users.team_id = charts.team_id
    AND users.workspace_hash = charts.workspace_hash
LEFT JOIN {{ ref('manager_workspace') }} AS workspace
    ON users.workspace_hash = workspace.workspace_hash
    AND users.team_id = workspace.team_id
