{{ config(materialized='table') }}

WITH MT AS (
  SELECT
    team.*,
    event_times.* EXCEPT (team_id, created_dttm)
  FROM {{ ref('manager_team_history') }} AS team
  LEFT JOIN {{ ref('time_to_event') }} AS event_times
    ON team.team_id = event_times.team_id
  WHERE team.ds = (SELECT MAX(ds) FROM {{ ref('manager_team_history') }})
)

SELECT
  MT.* EXCEPT (is_activated),
  MT.days_to_activated IS NOT NULL AS is_activated
FROM
  MT
