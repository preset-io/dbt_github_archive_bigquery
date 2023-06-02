{{
  config(
    alias='user_activity_periods'
  )
}}

/*
active users: The number of unique users active (at least once) in the last 28 days.
first-month activation rate: The proportion of new registrants from the last 28 days who have been active at least once.
month-to-month retention rate: The proportion of active users in the last 28 days among those who were active in the preceding 28-day period (i.e., between 55 and 28 days ago).
month-to-month reactivation rate: The proportion of active users in the last 28 days among those who were not active in the preceding 28-day period.
*/

WITH users AS (
  SELECT DISTINCT
    user_id,
    DATE(created_dttm) AS created_dt
  FROM
    {{ ref( 'manager_user' ) }}

)

SELECT
  DS.dt,
  COUNT(CURR.user_id) AS active_users,
  COUNT(CASE WHEN DS.dt <= U.created_dt + 27 AND CURR.user_id IS NOT NULL THEN 1 END) AS newly_active_users,
  COUNT(CASE WHEN DS.dt <= U.created_dt + 27 AND CURR.user_id IS NOT NULL THEN 1 END) /
      CAST(NULLIF(COUNT(CASE WHEN DS.dt <= U.created_dt + 27 THEN 1 END), 0) AS FLOAT64) AS activation_rate,
  COUNT(PREV.user_id) AS period_initial_users,
  COUNT(CASE WHEN PREV.user_id IS NOT NULL AND CURR.user_id IS NOT NULL THEN 1 END) AS retained_users,
  COUNT(PREV.user_id) - COUNT(PREV.user_id + CURR.user_id) AS churned_users,
  COUNT(CASE WHEN PREV.user_id IS NOT NULL AND CURR.user_id IS NOT NULL THEN 1 END) /
      CAST(NULLIF(COUNT(CASE WHEN PREV.user_id IS NOT NULL THEN 1 END), 0) AS FLOAT64) AS retention_rate,
  COUNT(CASE WHEN PREV.user_id IS NULL AND DS.dt - 28 >= U.created_dt AND CURR.user_id IS NOT NULL THEN 1 END) AS reactivated_users,
  COUNT(CASE WHEN PREV.user_id IS NULL AND DS.dt - 28 >= U.created_dt AND CURR.user_id IS NOT NULL THEN 1 END) /
      CAST(NULLIF(COUNT(CASE WHEN PREV.user_id IS NULL AND DS.dt - 28 >= U.created_dt THEN 1 END), 0) AS FLOAT64) AS reactivation_rate
FROM
  {{ ref( 'date_spine' ) }} AS DS
JOIN
  users AS U
ON DS.dt >= U.created_dt
LEFT JOIN
  {{ ref( 'user_trailing_activity_intervals' ) }} AS PREV
ON DS.dt - 28 BETWEEN PREV.start_dt AND PREV.end_dt
  AND U.user_id = PREV.user_id
LEFT JOIN
  {{ ref( 'user_trailing_activity_intervals' ) }} AS CURR
ON DS.dt BETWEEN CURR.start_dt AND CURR.end_dt
  AND U.user_id = CURR.user_id
WHERE
  DS.dt <= DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
GROUP BY 1
