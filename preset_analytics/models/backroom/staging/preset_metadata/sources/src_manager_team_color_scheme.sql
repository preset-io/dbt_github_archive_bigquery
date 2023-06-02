SELECT
    ds,
    id,
    created_dttm,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    scheme_id,
    colors,
    description,
    is_default,
    is_diverging,
    label,
    scheme_type,
    team_id,
FROM {{ source('production_preset_metadata', 'manager_team_color_scheme') }}
WHERE ds >= {{ var("start_date") }}
