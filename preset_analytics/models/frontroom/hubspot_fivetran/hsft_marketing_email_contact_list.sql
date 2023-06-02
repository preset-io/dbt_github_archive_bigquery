{{
  config(
    alias='marketing_email_contact_list',
    materialized='view'
  )
}}

SELECT
  contact_list_id,
  marketing_email_id,
  _fivetran_synced,
  is_mailing_list_included,
FROM
  {{ source('fivetran_hubspot', 'marketing_email_contact_list') }}
