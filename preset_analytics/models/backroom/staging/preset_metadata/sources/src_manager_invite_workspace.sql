SELECT
    ds,
    invite_id,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    role_identifier,
FROM {{ source('production_preset_metadata', 'manager_invite_workspace') }}
WHERE ds >= {{ var("start_date") }}
