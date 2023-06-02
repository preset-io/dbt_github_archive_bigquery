WITH embedded_dashboards AS (
  SELECT
    DS.dt,
    E.team_id,
    COUNT(DISTINCT e.url_path) AS embedded_dashboards_viewed
  FROM
    {{ ref( 'date_spine' ) }} AS DS
  LEFT JOIN
    {{ ref( 'wrk_superset_events' ) }} AS E
  ON DS.dt >= E.dt
  WHERE
    E.action = 'embedded'
  GROUP BY 1, 2
),

shared_dashboards AS (
  SELECT
    D.ds as dt,
    D.team_id,
    COUNT(DISTINCT D.dashboard_id ) AS shared_dashboards_viewed,
  FROM
    {{ ref( 'src_superset_dashboard' ) }} AS D
  LEFT JOIN
    {{ ref( 'wrk_action_actor_accounting_summary' ) }} AS AA
  ON D.ds = AA.ds
    AND D.team_id = AA.team_id
    AND CONCAT(D.workspace_id, '_', D.id) = AA.object_key
    AND AA.object_type = 'dashboard'
  WHERE
    NOT D.is_example
    AND AA.ltd_users > 1
  GROUP BY 1, 2
  ),

DV AS (
  SELECT
    MT.ds,
    MT.team_id,
    MAX(ED.embedded_dashboards_viewed) OVER (PARTITION BY MT.team_id ORDER BY MT.ds ASC ROWS UNBOUNDED PRECEDING) AS embedded_dashboards_viewed,
    MAX(SD.shared_dashboards_viewed) OVER (PARTITION BY MT.team_id ORDER BY MT.ds ASC ROWS UNBOUNDED PRECEDING) AS shared_dashboards_viewed
  FROM
    {{ ref( 'wrk_manager_team' ) }} AS MT
  LEFT JOIN
    embedded_dashboards AS ED
  ON MT.team_id = ED.team_id
    AND MT.ds = ED.dt
  LEFT JOIN
    shared_dashboards AS SD
  ON MT.team_id = SD.team_id
    AND MT.ds = SD.dt
  )

SELECT
  ds,
  team_id,
  embedded_dashboards_viewed,
  shared_dashboards_viewed,
  LEAST(100.0, 100.0 * GREATEST(COALESCE(embedded_dashboards_viewed, 0), COALESCE(shared_dashboards_viewed, 0)) / 5) AS onboarding_score
FROM
  DV
