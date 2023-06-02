{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}

WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this) }}
    {% endif %}
)

, epics AS (
  SELECT
    date_spine.dt,
    e.name AS epic_name,
    g.name AS team_name,
    r.name AS requested_by,
    e.owner_ids,
    e.label_ids,
    e.state,
    e.started_at,
    e.deadline,
    e.completed_at,
    e.is_started,
    e.is_completed,
    e.app_url,

    -- for use in downstream agg
    s.id AS story_id,
    s.is_completed AS story_is_completed,
  FROM date_spine
  LEFT JOIN {{ ref('latest_shortcut_epics') }} AS e
    ON date_spine.dt >= DATE(e.created_at)
  LEFT JOIN {{ ref('latest_shortcut_groups') }} AS g
    ON e.group_id = g.id
  LEFT JOIN {{ ref('latest_shortcut_stories') }} AS s
    ON e.id = s.epic_id
  LEFT JOIN {{ ref('latest_shortcut_members') }} AS r
    ON e.requested_by_id = r.id
)

, add_owner_names AS (
  SELECT e.* EXCEPT(owner_ids), COALESCE(m.name, 'none') AS owner_name
  FROM epics as e, unnest(owner_ids) AS owner_id
  LEFT JOIN {{ ref('latest_shortcut_members') }} AS m
    ON m.id = owner_id
)

, add_label_names AS (
  SELECT e.* EXCEPT(label_ids), COALESCE(l.name, 'none') AS label_name
  FROM add_owner_names AS e, unnest(label_ids) AS label_id
  LEFT JOIN {{ ref('latest_shortcut_labels') }} AS l
    ON l.id = SAFE_CAST(label_id AS int64)
)

SELECT
  dt,
  app_url,
  epic_name,
  team_name,
  requested_by,
  state,
  started_at,
  deadline,
  completed_at,
  is_started,
  is_completed,
  COUNT(story_id) AS num_stories,
  COUNT(CASE WHEN story_is_completed THEN story_id END) AS num_stories_completed,
  ARRAY_AGG(DISTINCT owner_name) AS owner_name_array,
  ARRAY_AGG(DISTINCT label_name) AS label_name_array,
FROM add_label_names
GROUP BY
  dt,
  app_url,
  epic_name,
  team_name,
  requested_by,
  state,
  started_at,
  deadline,
  completed_at,
  is_started,
  is_completed
