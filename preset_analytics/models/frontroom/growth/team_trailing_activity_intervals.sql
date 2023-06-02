{{
  config(
    alias='team_trailing_activity_intervals',
    materialized='table'
  )
}}


WITH event_log AS (
  SELECT DISTINCT
    dt AS activity_dt,
    manager_user_id AS user_id,
    team_id,
    workspace_id
  FROM
    {{ ref( 'superset_event_log' ) }}
  WHERE
    dt < CURRENT_DATE
    AND event_type = 'user' --still using user events to determine team actiity
  ),

events_set AS (
  SELECT
    team_id,
    activity_dt AS event_dt,
    1 AS days_active_delta
  FROM
    event_log

  UNION ALL

  SELECT
    team_id,
    activity_dt + 28 AS event_dt,
    -1 AS days_active_delta
  FROM
    event_log

  UNION ALL

  SELECT DISTINCT
    team_id,
    DATE(created_dttm) AS event_dt,
    0 AS days_active_delta
  FROM
    {{ ref( 'manager_team' ) }}
  ),

events AS (
  SELECT
    team_id,
    event_dt,
    SUM(days_active_delta) AS days_active_delta
  FROM
    events_set
  GROUP BY 1, 2
  ),

rolling_states AS (
  SELECT
    team_id,
    event_dt,
    SUM(days_active_delta) OVER (PARTITION BY team_id ORDER BY event_dt ASC ROWS UNBOUNDED PRECEDING) > 0 AS active
  FROM
    events
  ),

redundant_states AS (
  SELECT
    team_id,
    event_dt,
    active,
    COALESCE(active = LAG(active, 1) OVER (PARTITION BY team_id ORDER BY event_dt ASC), FALSE) AS redundant
  FROM
    rolling_states
  ),

deduplicated_states AS (
  SELECT
    team_id,
    event_dt,
    active
  FROM
    redundant_states
  WHERE
    NOT redundant
  ),

activity_times AS (
  SELECT
    team_id,
    event_dt AS start_dt,
    COALESCE(LEAD(event_dt, 1) OVER (PARTITION BY team_id ORDER BY event_dt ASC), CURRENT_DATE) - 1 AS end_dt,
    active
  FROM
    deduplicated_states
  )

SELECT
  team_id,
  start_dt,
  end_dt
FROM
  activity_times
WHERE
  active
