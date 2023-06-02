{{
  config(
    alias='email_event_dropped',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  bcc,
  cc,
  drop_message,
  drop_reason,
  email_event_dropped.from,
  reply_to,
  subject,
FROM
  {{ source('fivetran_hubspot', 'email_event_dropped') }}
