{{
  config(
    alias='email_event_sent',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  bcc,
  cc,
  email_event_sent.from,
  reply_to,
  subject,
FROM
  {{ source('fivetran_hubspot', 'email_event_sent') }} AS email_event_sent
