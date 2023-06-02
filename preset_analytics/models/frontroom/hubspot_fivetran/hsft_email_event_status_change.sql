{{
  config(
    alias='email_event_status_change',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  bounced,
  portal_subscription_status,
  requested_by,
  source,
  subscriptions,
FROM
  {{ source('fivetran_hubspot', 'email_event_status_change') }}
