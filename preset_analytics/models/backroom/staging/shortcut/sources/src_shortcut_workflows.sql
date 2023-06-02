SELECT
  id,
  team_id,
  {{ array_from_string('project_ids') }} AS project_ids,
  default_state_id,
  name,
  description,
  entity_type,
  states,
  auto_assign_owner AS is_auto_assign_owner,
  created_at,
  updated_at,
  loaded_at,
FROM {{ source('shortcut', 'workflows') }}
