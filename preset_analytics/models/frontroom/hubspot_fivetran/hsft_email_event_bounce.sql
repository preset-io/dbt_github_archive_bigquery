{{
  config(
    alias='email_event_bounce',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  category,
  response,
  status,
FROM
  {{ source('fivetran_hubspot', 'email_event_bounce') }}
