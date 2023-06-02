

SELECT
    ds,
    id,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    status,
    accepted_by_user_id,
    accepted_at,
    team_id,
    team_role_name,
    type,
FROM {{ ref('src_manager_invite_dedup') }}
WHERE ds >= {{ var("start_date") }}
