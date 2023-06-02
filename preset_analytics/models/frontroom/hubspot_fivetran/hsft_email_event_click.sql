{{
  config(
    alias='email_event_click',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  browser,
  ip_address,
  location,
  referer,
  url,
  user_agent,
FROM
  {{ source('fivetran_hubspot', 'email_event_click') }}
