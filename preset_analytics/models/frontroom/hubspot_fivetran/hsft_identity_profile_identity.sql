{{
  config(
    alias='identity_profile_identity',
    materialized='view'
  )
}}

SELECT
  identity_vid,
  value,
  _fivetran_synced,
  is_primary,
  is_secondary,
  timestamp,
  type,
FROM
  {{ source('fivetran_hubspot', 'identity_profile_identity') }}
