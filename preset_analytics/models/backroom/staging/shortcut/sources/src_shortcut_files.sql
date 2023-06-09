SELECT
  id,
  external_id,
  {{ array_from_string('story_ids') }} AS story_ids,
  uploader_id,
  {{ array_from_string('mention_ids') }} AS mention_ids,
  {{ array_from_string('member_mention_ids') }} AS member_mention_ids,
  {{ array_from_string('group_mention_ids') }} AS group_mention_ids,
  entity_type,
  content_type,
  name,
  filename,
  description,
  url,
  size,
  thumbnail_url,
  created_at,
  updated_at,
  loaded_at,
FROM {{ source('shortcut', 'files') }}
