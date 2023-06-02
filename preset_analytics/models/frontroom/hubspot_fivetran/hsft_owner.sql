{{
  config(
    alias='owner',
    materialized='view'
  )
}}

SELECT
  owner_id,
  _fivetran_synced,
  active_user_id,
  created_at,
  email,
  first_name,
  is_active,
  last_name,
  portal_id,
  type,
  updated_at,
  user_id_including_inactive,
FROM
  {{ source('fivetran_hubspot', 'owner') }}
