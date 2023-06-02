{{
  config(
    alias='email_event_open',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  browser,
  duration,
  ip_address,
  location,
  user_agent,
FROM
  {{ source('fivetran_hubspot', 'email_event_open') }}
