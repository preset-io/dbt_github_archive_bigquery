{{
  config(
    alias='marketing_email_contact',
    materialized='view'
  )
}}

SELECT
  contact_id,
  marketing_email_id,
  _fivetran_synced,
  is_contact_included,
FROM
  {{ source('fivetran_hubspot', 'marketing_email_contact') }}
