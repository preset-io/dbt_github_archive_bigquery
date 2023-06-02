{{ config(
    alias='wrk_user_persona_attributes_daily',
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'}
) }}

WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this) }}
    {% endif %}
)

, user_spine AS (
  SELECT DISTINCT
    team_id,
    manager_user_id AS user_id,
  FROM {{ ref('wrk_superset_events') }}
  WHERE DATE_TRUNC(CAST(dttm AS DATETIME), DAY) <= (SELECT MAX(dt) FROM date_spine)
)

, events AS (
  SELECT DISTINCT
    date_spine.dt,
    superset_events.team_id,
    superset_events.manager_user_id AS user_id,
    superset_events.event_id,
    CASE
      WHEN superset_events.action = 'explore' THEN 'Explorer Action'
      WHEN superset_events.action IN ('sql_json', 'sqllab', 'SqlLabRestApi.get_results') THEN 'SQL Action'
      WHEN superset_events.action = 'dashboard' THEN 'Dashboard Action'
    END AS activity_type,
  FROM date_spine
  LEFT JOIN {{ ref('wrk_superset_events') }} AS superset_events
    ON DATE_TRUNC(superset_events.dttm, DAY) = date_spine.dt
  WHERE superset_events.action IN ('explore', 'dashboard', 'sql_json', 'sqllab', 'SqlLabRestApi.get_results')
    AND superset_events.manager_user_id IS NOT NULL
)

SELECT
  date_spine.dt,
  user_spine.team_id,
  user_spine.user_id,
  COUNT(CASE activity_type WHEN 'Explorer Action' THEN event_id END)  AS num_explorer_activity,
  COUNT(CASE activity_type WHEN 'SQL Action' THEN event_id END) AS num_sql_activity,
  COUNT(CASE activity_type WHEN 'Dashboard Action' THEN event_id END) AS num_dashboard_activity,
FROM date_spine
CROSS JOIN user_spine
LEFT JOIN events
  ON events.dt = date_spine.dt
    AND events.team_id = user_spine.team_id
    AND events.user_id = user_spine.user_id
GROUP BY 1, 2, 3
