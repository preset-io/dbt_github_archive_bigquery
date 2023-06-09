{{
  config(
    alias='contact_merge_audit',
    materialized='view'
  )
}}

SELECT
  canonical_vid,
  contact_id,
  vid_to_merge,
  _fivetran_synced,
  entity_id,
  first_name,
  last_name,
  merged_from_email_selected,
  merged_from_email_source_id,
  merged_from_email_source_label,
  merged_from_email_source_type,
  merged_from_email_source_vids,
  merged_from_email_timestamp,
  merged_from_email_value,
  merged_to_email_selected,
  merged_to_email_source_id,
  merged_to_email_source_label,
  merged_to_email_source_type,
  merged_to_email_timestamp,
  merged_to_email_value,
  num_properties_moved,
  timestamp,
  user_id,
FROM
  {{ source('fivetran_hubspot', 'contact_merge_audit') }}
