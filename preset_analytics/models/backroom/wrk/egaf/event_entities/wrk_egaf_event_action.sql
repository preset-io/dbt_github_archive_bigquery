-- Tracking product verticals through common actions
WITH event_action AS (
  SELECT
    CASE
      /* sql_json got deprecated in favor of SqlLabRestApi.get_results */
      WHEN A.action IN ('sql_json', 'SqlLabRestApi.get_results') THEN 'SQLers'
      WHEN A.action = 'dashboard' THEN 'Dashboarders'
      WHEN A.action = 'explore'
        OR A.referrer LIKE '%app.preset.io/superset/explore/%'
        OR A.referrer LIKE '%app.preset.io/explore/%'
        THEN 'Explorers'
    END AS entity_type,
    CAST(A.dttm AS DATE) AS dt,
    A.team_id,
    A.workspace_hash,
    CAST(manager_user_id AS STRING) AS entity_id,
    CASE A.action
      WHEN 'explore' THEN B.is_example
      WHEN 'dashboard' THEN C.is_example
      ELSE False
    END AS is_example,
    A.event_id,
  FROM {{ ref('_wrk_egaf_event_base') }} AS A
  LEFT JOIN {{ ref('superset_slice_latest') }} AS B
    ON A.chart_key = B.chart_key
  LEFT JOIN {{ ref('superset_dashboard_latest') }} AS C
    ON A.dashboard_key = C.dashboard_key
  WHERE A.manager_user_id IS NOT NULL
)

SELECT *
FROM event_action
WHERE entity_type IS NOT NULL
