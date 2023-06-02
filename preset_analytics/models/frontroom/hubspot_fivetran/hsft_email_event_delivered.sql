{{
  config(
    alias='email_event_delivered',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  response,
  smtp_id,
FROM
  {{ source('fivetran_hubspot', 'email_event_delivered') }}
