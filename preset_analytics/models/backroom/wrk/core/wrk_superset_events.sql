{{ config(materialized='view') }}

WITH superset_events AS (
    SELECT
        CAST(A.dttm AS DATETIME) AS dttm,
        A.action,
        A.event_name,
        A.event_type,
        A.team_id,
        A.workspace,
        E.workspace_id,
        CASE WHEN A.dashboard_id IS NOT NULL THEN CONCAT(E.workspace_id, '_', A.dashboard_id) END AS dashboard_key,
        CASE WHEN A.slice_id IS NOT NULL THEN CONCAT(E.workspace_id, '_', A.slice_id) END AS chart_key,
        A.duration,
        A.environment,
        A.id AS event_id,
        A.referrer,
        COALESCE(
            SAFE_CAST(A.superset_user_id AS INT64),
            CASE WHEN SAFE_CAST(A.user_id AS INT64) > 0 THEN SAFE_CAST(A.user_id AS INT64) END
        ) AS superset_user_id,
        A.path AS url_path,
        CASE WHEN SAFE_CAST(A.manager_user_id AS INT64) > 0 THEN SAFE_CAST(A.manager_user_id AS INT64) END AS manager_user_id,
        A.source,
        A.source_id,
        A.ajs_user_id,
        A.anonymous_id,
        A.useragent,
        {{ database_engine_fields('engine') }}
    FROM {{ source('superset_events_production', 'superset_events') }} AS A
    LEFT JOIN {{ ref('manager_workspace') }} AS E
        ON A.workspace = E.workspace_hash
    WHERE CAST(A.dttm AS DATETIME) < CURRENT_DATE()
),

user_mapping AS (
    SELECT
        workspace_id,
        superset_user_id,
        MAX(manager_user_id) AS manager_user_id,
    FROM superset_events
    WHERE manager_user_id IS NOT NULL
        AND workspace_id IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    CAST(events.dttm AS DATE) AS dt,
    events.dttm,
    events.action,
    events.event_name,
    events.event_type,
    events.team_id,
    events.workspace,
    events.workspace_id,
    events.dashboard_key,
    events.chart_key,
    events.duration,
    events.environment,
    events.event_id,
    events.referrer,
    events.superset_user_id,
    events.url_path,
    COALESCE(map.manager_user_id, events.manager_user_id) AS manager_user_id,
    events.source,
    events.source_id,
    events.database_engine,
    events.database_driver,
    events.ajs_user_id,
    events.anonymous_id,
    events.useragent,
FROM superset_events AS events
LEFT JOIN user_mapping AS map
  ON map.workspace_id = events.workspace_id
    AND map.superset_user_id = events.superset_user_id
