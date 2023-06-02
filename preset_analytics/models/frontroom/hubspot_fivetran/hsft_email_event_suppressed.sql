{{
  config(
    alias='email_event_suppressed',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  bcc,
  cc,
  email_campaign_group_id,
  email_event_suppressed.from,
  reply_to,
  subject,
FROM
  {{ source('fivetran_hubspot', 'email_event_suppressed') }} AS email_event_suppressed
