SELECT
  id,
  {{ array_from_string('project_ids') }} AS project_ids,
  entity_type,
  name,
  description,
  position,
  workflow,
  created_at,
  updated_at,
  loaded_at,
FROM {{ source('shortcut', 'teams') }}
