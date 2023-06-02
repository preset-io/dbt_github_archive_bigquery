SELECT
    ds,
    id,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    last_accessed_at,
    name,
    title,
    hostname,
    description,
    team_id,
    allow_public_dashboards,
    icon,
    color,
    status,
FROM {{ ref('src_manager_workspace_dedup') }}
WHERE ds >= {{ var("start_date") }}
