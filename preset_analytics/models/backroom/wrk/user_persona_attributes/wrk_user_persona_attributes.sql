{{ config(
    alias='wrk_user_persona_attributes',
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

, min_max_dates AS (
  SELECT
    date_spine.dt,
    events.user_id,
    events.team_id,
    MIN(CASE WHEN COALESCE(num_explorer_activity,0) > 0 THEN events.dt END) AS first_explorer_activity_dt,
    MAX(CASE WHEN COALESCE(num_explorer_activity,0) > 0 THEN events.dt END) AS latest_explorer_activity_dt,
    MIN(CASE WHEN COALESCE(num_sql_activity,0) > 0 THEN events.dt END) AS first_sql_activity_dt,
    MAX(CASE WHEN COALESCE(num_sql_activity,0) > 0 THEN events.dt END) AS latest_sql_activity_dt,
    MIN(CASE WHEN COALESCE(num_dashboard_activity, 0) > 0 THEN events.dt END) AS first_dashboard_activity_dt,
    MAX(CASE WHEN COALESCE(num_dashboard_activity, 0) > 0 THEN events.dt END) AS latest_dashboard_activity_dt,
  FROM date_spine
  LEFT JOIN {{ ref('wrk_user_persona_attributes_daily') }} AS events
    ON events.dt <= date_spine.dt
  GROUP BY 1, 2, 3
)

SELECT
  date_spine.dt,
  daily_atts.user_id,
  daily_atts.team_id,
  daily_atts.num_explorer_activity,
  daily_atts.num_sql_activity,
  daily_atts.num_dashboard_activity,
  cummulative_atts.to_date_num_explorer_activity,
  cummulative_atts.to_date_num_sql_activity,
  cummulative_atts.to_date_num_dashboard_activity,
  min_max_dates.first_explorer_activity_dt,
  min_max_dates.latest_explorer_activity_dt,
  min_max_dates.first_sql_activity_dt,
  min_max_dates.latest_sql_activity_dt,
  min_max_dates.first_dashboard_activity_dt,
  min_max_dates.latest_dashboard_activity_dt,
  cummulative_atts.to_date_num_explorer_activity > 0 AS has_explorer_activity,
  cummulative_atts.to_date_num_sql_activity > 0 AS has_sql_activity,
  cummulative_atts.to_date_num_dashboard_activity > 0 AS has_dashboard_activity,
FROM date_spine
LEFT JOIN {{ ref('wrk_user_persona_attributes_daily') }} AS daily_atts
  ON date_spine.dt = daily_atts.dt
LEFT JOIN {{ ref('wrk_user_persona_attributes_cummulative') }} AS cummulative_atts
  ON date_spine.dt = cummulative_atts.dt
    AND daily_atts.user_id = cummulative_atts.user_id
    AND daily_atts.team_id = cummulative_atts.team_id
LEFT JOIN min_max_dates
  ON date_spine.dt = min_max_dates.dt
    AND daily_atts.user_id = min_max_dates.user_id
    AND daily_atts.team_id = min_max_dates.team_id
