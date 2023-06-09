SELECT
  id,
  global_id,
  {{ array_from_string('group_ids') }} AS group_ids,
  entity_type,
  profile,
  REGEXP_EXTRACT(profile, r"'name': '(.+?)'") AS name,
  role,
  state,
  disabled AS is_disabled,
  created_without_invite AS is_created_without_invite,
  created_at,
  updated_at,
  loaded_at,
FROM {{ source('shortcut', 'members') }}
