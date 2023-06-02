{{
  config(
    alias='contact_list',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_deleted,
  _fivetran_synced,
  created_at,
  deleteable,
  dynamic,
  metadata_error,
  metadata_last_processing_state_change_at,
  metadata_last_size_change_at,
  metadata_processing,
  metadata_size,
  name,
  offset,
  portal_id,
  updated_at,
FROM
  {{ source('fivetran_hubspot', 'contact_list') }}
