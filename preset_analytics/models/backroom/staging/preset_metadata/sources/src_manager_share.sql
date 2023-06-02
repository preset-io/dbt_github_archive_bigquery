SELECT
  ds,
  id,
  created_dttm,
  last_modified_dttm,
  creator_user_id,
  last_modified_user_id,
  entity_type,
  entity_id,
  workspace_id,
  user_id,
  invite_id,
FROM {{ source('production_preset_metadata', 'manager_share') }}
WHERE ds >= {{ var("start_date") }}
