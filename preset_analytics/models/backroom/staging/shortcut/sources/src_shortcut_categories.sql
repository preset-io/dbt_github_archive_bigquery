SELECT
  id,
  external_id,
  entity_type,
  name,
  color,
  type,
  archived AS is_archieved,
  created_at,
  updated_at,
  loaded_at,
FROM {{ source('shortcut', 'categories') }}
