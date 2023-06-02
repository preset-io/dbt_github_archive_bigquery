SELECT
  ds,
  id,
  created_dttm,
  last_modified_dttm,
  creator_user_id,
  last_modified_user_id,
  uuid,
  name,
  team_id,
  team_role_id,
FROM {{ source('production_preset_metadata', 'manager_user_group') }}
WHERE ds >= {{ var("start_date") }}
