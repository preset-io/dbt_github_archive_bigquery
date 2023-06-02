SELECT
  ds,
  user_id,
  group_id,
  created_dttm,
  last_modified_dttm,
  creator_user_id,
  last_modified_user_id,
FROM {{ source('production_preset_metadata', 'manager_user_group_membership') }}
WHERE ds >= {{ var("start_date") }}
