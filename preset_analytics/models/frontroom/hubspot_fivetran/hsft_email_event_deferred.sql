{{
  config(
    alias='email_event_deferred',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  attempt,
  response,
FROM
  {{ source('fivetran_hubspot', 'email_event_deferred') }}
