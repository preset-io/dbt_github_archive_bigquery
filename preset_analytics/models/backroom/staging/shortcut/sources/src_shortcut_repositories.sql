SELECT
  id,
  external_id,
  entity_type,
  name,
  full_name,
  type,
  url,
  created_at,
  updated_at,
  loaded_at,
FROM {{ source('shortcut', 'repositories') }}
