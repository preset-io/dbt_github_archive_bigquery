{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    full_refresh = var("super_full_refresh_use_with_care"),
    partition_by = {'field': 'ds', 'data_type': 'date'}
) }}

WITH date_spine AS (
    SELECT
        dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this, this_date_col='ds') }}
    {% endif %}
)

, lifetime_agg AS (
  SELECT
    date_spine.dt AS ds,
    A.team_id,
    A.object_key,
    A.object_type,
    A.action,
    SUM(A.num_daily_views) AS num_views,
    ARRAY_CONCAT_AGG(A.daily_users_array) AS users_array,
  FROM date_spine
  LEFT JOIN {{ ref('wrk_action_actor_accounting_daily') }} AS A
    ON date_spine.dt >= A.ds
  GROUP BY 1, 2, 3, 4, 5
)

{% for i in var("day_counts") %}
, n{{ i }}_agg AS (
  SELECT
    date_spine.dt AS ds,
    A.team_id,
    A.object_key,
    A.object_type,
    A.action,
    SUM(A.num_daily_views) AS num_views,
    ARRAY_CONCAT_AGG(A.daily_users_array) AS users_array,
  FROM date_spine
  LEFT JOIN {{ ref('wrk_action_actor_accounting_daily') }} AS A
    ON A.ds >= DATE_ADD(date_spine.dt, INTERVAL -{{ i }} DAY)
  GROUP BY 1, 2, 3, 4, 5
)
{% endfor %}

SELECT
  ltd.ds,
  ltd.team_id,
  ltd.object_key,
  ltd.object_type,
  ltd.action,

  ltd.num_views AS ltd_views,
  (SELECT ARRAY_AGG(DISTINCT x) FROM UNNEST(ltd.users_array) AS x) AS ltd_users_array,
  (SELECT COUNT(DISTINCT x) FROM UNNEST(ltd.users_array) AS x) AS ltd_users,

  {% for i in var("day_counts") %}
      n{{ i }}_agg.num_views AS n{{ i }}d_views,
      (SELECT ARRAY_AGG(DISTINCT x) FROM UNNEST(n{{ i }}_agg.users_array) AS x) AS n{{ i }}d_users_array,
      (SELECT COUNT(DISTINCT x) FROM UNNEST(n{{ i }}_agg.users_array) AS x) AS n{{ i }}d_users,
  {% endfor %}
FROM lifetime_agg AS ltd
{% for i in var("day_counts") %}
LEFT JOIN n{{ i }}_agg
  ON ltd.ds = n{{ i }}_agg.ds
    AND ltd.team_id = n{{ i }}_agg.team_id
    AND ltd.object_key = n{{ i }}_agg.object_key
    AND ltd.object_type = n{{ i }}_agg.object_type
    AND ltd.action = n{{ i }}_agg.action
{% endfor %}
