{{
  config(
    alias='team_activity_periods'
  )
}}

/*
active teams: The number of unique teams with users active (at least once) in the last 28 days.
first-month activation rate: The proportion of new team registrants from the last 28 days who have been active at least once.
month-to-month retention rate: The proportion of active teams in the last 28 days among those who were active in the preceding 28-day period (i.e., between 55 and 28 days ago).
month-to-month reactivation rate: The proportion of active teams in the last 28 days among those who were not active in the preceding 28-day period.
*/

WITH teams AS (
  SELECT DISTINCT
    ds,
    team_id,
    DATE(created_dttm) AS created_dt,
    is_activated,
    team_billing_status,
    is_preset
  FROM
    {{ ref( 'manager_team_history' ) }}

)

SELECT
  DS.dt,
  T.is_activated,
  T.team_billing_status,
  T.is_preset,
  COUNT(CASE WHEN CURR.team_id IS NOT NULL THEN 1 END) AS active_teams,
  COUNT(CASE WHEN DS.dt <= T.created_dt + 27 AND CURR.team_id IS NOT NULL THEN 1 END) AS newly_active_teams,
  COUNT(CASE WHEN DS.dt <= T.created_dt + 27 AND CURR.team_id IS NOT NULL THEN 1 END) /
      CAST(NULLIF(COUNT(CASE WHEN DS.dt <= T.created_dt + 27 THEN 1 END), 0) AS FLOAT64) AS activation_rate,
  CAST(NULLIF(COUNT(CASE WHEN PREV.team_id IS NOT NULL THEN 1 END), 0) AS FLOAT64) AS period_initial_teams,
  COUNT(CASE WHEN PREV.team_id IS NOT NULL AND CURR.team_id IS NOT NULL THEN 1 END) AS retained_teams,
  CAST(NULLIF(COUNT(CASE WHEN PREV.team_id IS NOT NULL THEN 1 END), 0) AS FLOAT64) - COUNT(CASE WHEN PREV.team_id IS NOT NULL AND CURR.team_id IS NOT NULL THEN 1 END) AS churned_teams,
  COUNT(CASE WHEN PREV.team_id IS NOT NULL AND CURR.team_id IS NOT NULL THEN 1 END) /
      CAST(NULLIF(COUNT(CASE WHEN PREV.team_id IS NOT NULL THEN 1 END), 0) AS FLOAT64) AS retention_rate,
  COUNT(CASE WHEN PREV.team_id IS NULL AND DS.dt - 28 >= T.created_dt AND CURR.team_id IS NOT NULL THEN 1 END) AS reactivated_teams,
  COUNT(CASE WHEN PREV.team_id IS NULL AND DS.dt - 28 >= T.created_dt AND CURR.team_id IS NOT NULL THEN 1 END) /
      CAST(NULLIF(COUNT(CASE WHEN PREV.team_id IS NULL AND DS.dt - 28 >= T.created_dt THEN 1 END), 0) AS FLOAT64) AS reactivation_rate
FROM
  {{ ref( 'date_spine' ) }} AS DS
JOIN
  teams AS T
ON DS.dt = T.ds
  AND DS.dt >= T.created_dt
LEFT JOIN
  {{ ref( 'team_trailing_activity_intervals' ) }} AS PREV
ON DS.dt - 28 BETWEEN PREV.start_dt AND PREV.end_dt
  AND T.team_id = PREV.team_id
LEFT JOIN
  {{ ref( 'team_trailing_activity_intervals' ) }} AS CURR
ON DS.dt BETWEEN CURR.start_dt AND CURR.end_dt
  AND T.team_id = CURR.team_id
WHERE
  DS.dt <= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
GROUP BY 1, 2, 3, 4
