SELECT
  ds,
  id,
  entity_id,
  created_dttm,
  last_modified_dttm,
  creator_user_id,
  last_modified_user_id,
  name,
  LOWER(email) AS email,
  workspace_id,
FROM {{ source('production_preset_metadata', 'manager_vendor_connection') }}
WHERE ds >= {{ var("start_date") }}
