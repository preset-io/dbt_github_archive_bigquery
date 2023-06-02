{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}

WITH activity as (
  SELECT
    DATE_TRUNC(dt, DAY) AS dt,
    team_name,
    team_id,
    SUM(CASE WHEN entity_type = 'Dashboarders' THEN weekly_active_entity ELSE 0 END) AS weekly_active_dashboarders,
    SUM(CASE WHEN entity_type in ('Explorers','SQLers') THEN weekly_active_entity ELSE 0 END) AS weekly_active_explorers,
    SUM(CASE WHEN entity_type = 'dashboard' THEN visits_28d ELSE 0 END) AS dashboard_visits_28d
  FROM
    {{ ref( 'egaf_active_entity' ) }}
  GROUP BY 1, 2, 3
),

team_billing as (
  SELECT
    team_name,
    team_id,
    team_billing_status_derived as billing,
    CASE WHEN team_billing_status = 'FREE' THEN 1
         WHEN team_billing_status = 'TRIAL_EXPIRED' THEN 2
         WHEN team_billing_status = 'OVERDUE' THEN 3
         WHEN team_billing_status = 'TRIAL' THEN 4
         WHEN team_billing_status = 'PAID' THEN 5
         WHEN team_billing_status = 'ENTERPRISE_TRIAL' THEN 6
         WHEN team_billing_status = 'ENTERPRISE' THEN 7
         END as billing_level
  FROM
    {{ ref( 'manager_team' ) }}
  ),

primary_billing as (
-- get billing information since I'm not sure if team_billing_status_derived or team_billing_status is the source of truth
  SELECT
    team_name,
    team_id,
    CASE WHEN MAX(billing_level) = 7 THEN 'ENTERPRISE'
         WHEN MAX(billing_level) = 6 THEN 'ENTERPRISE_TRIAL'
         WHEN MAX(billing_level) = 5 THEN 'PAID'
         WHEN MAX(billing_level) = 4 THEN 'TRIAL'
         WHEN MAX(billing_level) = 3 THEN 'OVERDUE'
         WHEN MAX(billing_level) = 2 THEN 'TRIAL_EXPIRED'
         WHEN MAX(billing_level) = 1 THEN 'FREE'
         END as billing
  FROM
    team_billing
  GROUP BY 1, 2
  ),

max_mau_team AS (
  SELECT
    team_name,
    team_id,
    MAX(mau) as mau
  FROM
    {{ ref( 'manager_team_history' ) }}
  WHERE
    DATE_TRUNC(ds, DAY) = CURRENT_DATE - 7
  GROUP BY 1, 2
  ),

primary_team as (
-- get primary team based on the team with the largest MAU a week ago
  SELECT
    MTH.team_name,
    MTH.team_id
  FROM
    {{ ref( 'manager_team_history' ) }} as MTH
  JOIN
    max_mau_team
  ON MTH.team_id = max_mau_team.team_id
  WHERE
    MTH.mau = max_mau_team.mau
    AND DATE_TRUNC(MTH.ds, DAY) = CURRENT_DATE - 7
),

seats as (
  SELECT
    DATE_TRUNC(MTH.ds, DAY) AS dt,
    MTH.team_name,
    MTH.team_id,
    billing,
    SUM(MTH.mau) as mau,
    SUM(MTH.team_members) as user_accounts,
    SUM(MTH.mau) / SUM(MTH.team_members) AS total_MAU_assigned_seats,
  FROM
    primary_team
  LEFT JOIN
    {{ ref( 'manager_team_history' ) }} as MTH
  ON primary_team.team_id = MTH.team_id
  LEFT JOIN
    primary_billing
  ON primary_team.team_id = primary_billing.team_id
  WHERE
    MTH.is_preset IS NULL
    OR NOT MTH.is_preset
  GROUP BY 1, 2, 3, 4
),

metrics as (
  SELECT
    activity.dt,
    activity.team_name,
    activity.team_id,
    seats.billing,
    activity.weekly_active_dashboarders,
    activity.weekly_active_explorers,
    activity.dashboard_visits_28d,
    seats.total_MAU_assigned_seats,
    seats.user_accounts,
    seats.mau
  FROM
    activity
  JOIN
    seats
  ON activity.team_id = seats.team_id AND activity.dt = seats.dt
),

seven_average as (
  SELECT
    B.dt,
    B.team_name,
    B.team_id,
    B.billing,
    AVG(A.weekly_active_dashboarders) as weekly_active_dashboarders_7d_avg,
    AVG(A.weekly_active_explorers) as weekly_active_explorers_7d_avg,
    AVG(A.dashboard_visits_28d) as dashboard_visits_28d_7d_avg,
    AVG(A.total_MAU_assigned_seats) as total_MAU_assigned_seats_7d_avg,
    ROUND(AVG(A.user_accounts), 0) as user_accounts_7d_avg,
    ROUND(AVG(A.mau), 0) as mau_7d_avg
  FROM
    metrics as A
  JOIN
    metrics as B
  ON A.dt <= B.dt and A.dt >= B.dt - 6 and A.team_id = B.team_id
  GROUP BY 1,2,3,4
  ),

percent_calc as (
  SELECT
    B.dt,
    B.team_name,
    B.team_id,
    B.billing,
    B.user_accounts_7d_avg,
    B.mau_7d_avg,
    A.weekly_active_dashboarders_7d_avg as WADashboarders_28d_ago,
    B.weekly_active_dashboarders_7d_avg as WADashboarders,
    B.weekly_active_dashboarders_7d_avg / NULLIF(A.weekly_active_dashboarders_7d_avg, 0) - 1 as WADashboarders_28d_change,
    A.weekly_active_explorers_7d_avg as WACreators_28d_ago,
    B.weekly_active_explorers_7d_avg as WACreators,
    B.weekly_active_explorers_7d_avg / NULLIF(A.weekly_active_explorers_7d_avg, 0) - 1 as WACreators_28d_change,
    A.dashboard_visits_28d_7d_avg as dash_visits_28d_28d_ago,
    B.dashboard_visits_28d_7d_avg as dash_visits_28d,
    B.dashboard_visits_28d_7d_avg / NULLIF(a.dashboard_visits_28d_7d_avg, 0) - 1 as dash_visits_28d_28d_change,
    A.total_MAU_assigned_seats_7d_avg as MAU_seats_28ago,
    B.total_MAU_assigned_seats_7d_avg as MAU_seats,
    B.total_MAU_assigned_seats_7d_avg / NULLIF(A.total_MAU_assigned_seats_7d_avg, 0) - 1 as MAU_Seats_28d_change
  FROM
    seven_average as A
  JOIN
    seven_average as B
  ON A.team_id = B.team_id AND A.dt = B.dt - 28
  WHERE
    B.dt = CURRENT_DATE - 2
  ),

whataction as (
  SELECT
    * EXCEPT (WADashboarders_28d_change, WACreators_28d_change, dash_visits_28d_28d_change, MAU_Seats_28d_change),
    ROUND(WADashboarders_28d_change, 6) AS WADashboarders_28d_change,
    ROUND(WACreators_28d_change, 6) AS WACreators_28d_change,
    ROUND(dash_visits_28d_28d_change, 6) AS dash_visits_28d_28d_change,
    ROUND(MAU_Seats_28d_change, 6) AS MAU_Seats_28d_change,
    CASE WHEN WADashboarders_28d_change < -0.1 OR WADashboarders_28d_change IS NULL THEN -1
         WHEN WADashboarders_28d_change > 0.1 THEN 1
         ELSE 0
         END +
      CASE WHEN WACreators_28d_change < -0.1 OR WACreators_28d_change IS NULL THEN -1
           WHEN WACreators_28d_change > 0.1 THEN 1
           ELSE 0
           END +
      CASE WHEN dash_visits_28d_28d_change < -0.1 OR dash_visits_28d_28d_change IS NULL THEN -1
           WHEN dash_visits_28d_28d_change > 0.1 THEN 1
           ELSE 0
           END +
      CASE WHEN MAU_Seats_28d_change < -0.1 OR MAU_Seats_28d_change IS NULL THEN -1
           WHEN MAU_Seats_28d_change > 0.1 THEN 1
           ELSE 0
           END as scoring
  FROM
    percent_calc
),

VT as (
  SELECT
    *,
    CASE WHEN scoring <= -2 THEN 'Churn Risk'
         WHEN scoring >= 2 THEN 'Upsell Opp'
         END as rating
  FROM
    whataction
)

SELECT
  *
FROM
  VT
WHERE
  ((billing LIKE '%ENTERPRISE%'
        AND billing NOT LIKE '%ENTERPRISE_TRIAL%')
      OR user_accounts_7d_avg > 5)
{% if is_incremental() %}
  AND {{ generate_incremental_statement(this) }}
{% endif %}
