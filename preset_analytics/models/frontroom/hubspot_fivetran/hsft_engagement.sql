{{
  config(
    alias='engagement',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  active,
  activity_type,
  created_at,
  last_updated,
  owner_id,
  portal_id,
  timestamp,
  type,
FROM
  {{ source('fivetran_hubspot', 'engagement') }}
