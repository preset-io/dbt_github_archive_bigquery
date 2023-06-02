{{
  config(
    alias='email_event',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  app_id,
  caused_by_created,
  caused_by_id,
  created,
  email_campaign_id,
  filtered_event,
  obsoleted_by_created,
  obsoleted_by_id,
  portal_id,
  recipient,
  sent_by_created,
  sent_by_id,
  type,
FROM
  {{ source('fivetran_hubspot', 'email_event') }}
