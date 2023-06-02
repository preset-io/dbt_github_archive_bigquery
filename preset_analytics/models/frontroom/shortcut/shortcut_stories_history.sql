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

, stories AS (
  SELECT
    date_spine.dt,

    s.*,
    s.story_name AS name,

    r.name AS requested_by,
    e.name AS epic_name,
    t.name AS team_name,
  FROM date_spine
  LEFT JOIN {{ ref('latest_shortcut_stories') }} AS s
    ON date_spine.dt >= DATE(s.created_at)
  LEFT JOIN {{ ref('latest_shortcut_epics') }} AS e
    ON s.epic_id = e.id
  LEFT JOIN {{ ref('latest_shortcut_workflows') }} AS w
    ON s.workflow_id = w.id
  LEFT JOIN {{ ref('latest_shortcut_teams') }} AS t
    ON w.team_id = t.id
  LEFT JOIN {{ ref('latest_shortcut_members') }} AS r
    ON s.requested_by_id = r.id
)

, add_label_names AS (
  SELECT s.* EXCEPT(label_ids), COALESCE(l.name, 'none') AS label_name
  FROM stories as s, unnest(label_ids) AS label_id
  LEFT JOIN {{ ref('latest_shortcut_labels') }} AS l
    ON l.id = SAFE_CAST(label_id AS int64)
)

, add_owner_names AS (
  SELECT s.* EXCEPT(owner_ids), COALESCE(m.name, 'none') AS owner_name
  FROM add_label_names as s, unnest(owner_ids) AS owner_id
  LEFT JOIN {{ ref('latest_shortcut_members') }} AS m
    ON m.id = owner_id
)

SELECT
  dt,
  app_url,
  story_name,
  project_id,
  story_type,
  created_at,
  requested_by,
  epic_name,
  team_name,
  is_started,
  is_completed,
  DATE_DIFF(dt, DATE(created_at), DAY) AS age,
  ARRAY_AGG(DISTINCT label_name) AS label_name_array,
  ARRAY_AGG(DISTINCT owner_name) AS owner_name_array,
FROM add_owner_names
GROUP BY
  dt,
  app_url,
  story_name,
  project_id,
  story_type,
  created_at,
  requested_by,
  epic_name,
  team_name,
  is_started,
  is_completed
