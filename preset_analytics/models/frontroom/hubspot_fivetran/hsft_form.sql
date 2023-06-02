{{
  config(
    alias='form',
    materialized='view'
  )
}}

SELECT
  guid,
  _fivetran_deleted,
  _fivetran_synced,
  action,
  created_at,
  css_class,
  follow_up_id,
  lead_nurturing_campaign_id,
  method,
  name,
  notify_recipients,
  portal_id,
  redirect,
  submit_text,
  updated_at,
FROM
  {{ source('fivetran_hubspot', 'form') }}
