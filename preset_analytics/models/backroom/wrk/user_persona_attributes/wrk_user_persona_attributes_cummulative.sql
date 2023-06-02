{{ config(
    alias='wrk_user_persona_attributes_cummulative',
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

, daily_event_counts AS (
  SELECT DISTINCT
    date_spine.dt,
    events.user_id,
    events.team_id,
    events.num_explorer_activity,
    events.num_sql_activity,
    events.num_dashboard_activity,
  FROM date_spine
  LEFT JOIN {{ ref('wrk_user_persona_attributes_daily') }} AS events
    ON events.dt = date_spine.dt

  {% if is_incremental() %}
    UNION ALL

    -- union the last previously calculated value to limit scope of window function
    SELECT
      dt,
      user_id,
      team_id,
      to_date_num_explorer_activity AS num_explorer_activity,
      to_date_num_sql_activity AS num_sql_activity,
      to_date_num_dashboard_activity AS num_dashboard_activity,
    FROM {{ this }}
    WHERE dt = date_sub(current_date, interval 3 day)
  {% endif %}
)

SELECT DISTINCT
  dt,
  user_id,
  team_id,
  SUM(num_explorer_activity) OVER (PARTITION BY user_id, team_id ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_num_explorer_activity,
  SUM(num_sql_activity) OVER (PARTITION BY user_id, team_id ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_num_sql_activity,
  SUM(num_dashboard_activity) OVER (PARTITION BY user_id, team_id ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_num_dashboard_activity,
FROM daily_event_counts
