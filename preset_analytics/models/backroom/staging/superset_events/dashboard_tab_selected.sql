{{
  config(
    alias='dashboard_tab_selected'
  )
}}

SELECT
    id,
    action,
    context_library_name,
    context_library_version,
    dashboard_id,
    dttm,
    duration,
    environment,
    event,
    event_text,
    loaded_at,
    original_timestamp,
    received_at,
    referrer,
    sent_at,
    slice_id,
    team_id,
    timestamp,
    user_id,
    uuid_ts,
    workspace,
    path,
    event_name,
    event_type,
    manager_user_id,
    source,
    source_id,
    engine,
    app_key_name,
    superset_user_id
FROM
    {{ source('superset_events_production', 'superset_events') }}
WHERE
    action = 'log'
    AND event_name = 'select_dashboard_tab'
