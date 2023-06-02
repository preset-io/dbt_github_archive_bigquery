{{
  config(
    alias='email_event_spam_report',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  ip_address,
  user_agent,
FROM
  {{ source('fivetran_hubspot', 'email_event_spam_report') }}
