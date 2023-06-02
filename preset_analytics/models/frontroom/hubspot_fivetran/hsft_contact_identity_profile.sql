{{
  config(
    alias='contact_identity_profile',
    materialized='view'
  )
}}

SELECT
  contact_id,
  vid,
  _fivetran_synced,
  deleted_changed_timestamp,
  saved_at_timestamp,
FROM
  {{ source('fivetran_hubspot', 'contact_identity_profile') }}
