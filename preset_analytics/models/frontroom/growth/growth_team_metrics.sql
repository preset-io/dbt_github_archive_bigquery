{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'}
) }}

WITH date_spine AS (
    SELECT
        dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this) }}
    {% endif %}
)

, manager_team_summary AS (
    SELECT
        DATE(ds) AS dt,
        COUNT(team_id) AS num_teams_total,
        COUNT(CASE WHEN is_activated THEN team_id END) AS num_teams_activated,
        COUNT(CASE WHEN non_example_database_count > 0 THEN team_id END) AS num_teams_w_non_ex_dbs, -- % Teams >0 Non-Example Databases
        COUNT(CASE WHEN chart_noex_count > 0 THEN team_id END) AS num_teams_w_non_ex_visuals, -- % Teams >0 Non-Example Visualization
        COUNT(CASE WHEN dashboard_noex_count > 0 THEN team_id END) AS num_teams_w_non_ex_dashboards_created, -- % Teams >0 Non-Example Dashboards Created
        COUNT(CASE WHEN invite_sent > 0 THEN team_id END) AS num_teams_w_invited_user, -- % Teams >0 Invited Users
        COUNT(CASE WHEN team_members > 1 THEN team_id END) AS num_teams_w_greater_than_one_registerd_users, -- % Teams >1 Registered Users
        COUNT(CASE WHEN da_dashboard > 0 THEN team_id END) AS num_teams_w_dashboards_w_viewers, -- % Teams >0 Non-Example Dashboards With >1 Viewer
    FROM {{ ref('manager_team_history') }}
    WHERE NOT is_preset
    GROUP BY 1
)

, billing_status_summary AS (
    SELECT
        date_spine.dt,
        SUM(CASE WHEN status_history.billing_status IN ('PAID', 'ENTERPRISE') THEN 1 ELSE 0 END) AS num_paid_teams,
        SUM(CASE WHEN status_history.billing_status IN ('TRIAL') THEN 1 ELSE 0 END) AS num_trial_teams,
    FROM date_spine
    LEFT JOIN {{ ref('manager_team_billing_status_history') }} AS status_history
    ON date_spine.dt >= status_history.effective_from
        AND date_spine.dt < COALESCE(status_history.effective_to, '3000-01-01')
    GROUP BY 1
)

SELECT
    date_spine.dt,
    manager_team_summary.num_teams_total,
    manager_team_summary.num_teams_activated,
    manager_team_summary.num_teams_w_non_ex_dbs,
    manager_team_summary.num_teams_w_non_ex_visuals,
    manager_team_summary.num_teams_w_non_ex_dashboards_created,
    manager_team_summary.num_teams_w_invited_user,
    manager_team_summary.num_teams_w_greater_than_one_registerd_users,
    manager_team_summary.num_teams_w_dashboards_w_viewers,
    billing_status_summary.num_paid_teams,
    billing_status_summary.num_trial_teams,
FROM date_spine
LEFT JOIN billing_status_summary
    ON billing_status_summary.dt = date_spine.dt
LEFT JOIN manager_team_summary
    ON manager_team_summary.dt = date_spine.dt
