SELECT
    ds,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    user_id,
    team_id,
    team_role_name,
FROM {{ ref('src_manager_team_membership_dedup') }}
WHERE ds >= {{ var("start_date") }}
