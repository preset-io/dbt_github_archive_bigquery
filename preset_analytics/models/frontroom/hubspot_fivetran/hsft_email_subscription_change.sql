{{
  config(
    alias='email_subscription_change',
    materialized='view'
  )
}}

SELECT
  recipient,
  timestamp,
  _fivetran_id,
  _fivetran_synced,
  caused_by_event_id,
  change,
  change_type,
  email_subscription_id,
  portal_id,
  source,
FROM
  {{ source('fivetran_hubspot', 'email_subscription_change') }}
