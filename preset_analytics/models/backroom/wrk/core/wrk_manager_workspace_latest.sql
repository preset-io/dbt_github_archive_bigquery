SELECT
    ds,
    workspace_id,
    workspace_hash,
    workspace_title,
    workspace_hostname,
    workspace_region,
    workspace_description,
    allow_public_dashboards,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    last_accessed_at,
    team_id,
FROM {{ ref('wrk_manager_workspace') }}
WHERE ds = (SELECT MAX(ds) FROM {{ ref('wrk_manager_workspace') }})
