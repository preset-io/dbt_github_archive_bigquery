{{
  config(
    alias='contact_form_submission',
    materialized='view'
  )
}}

SELECT
  contact_id,
  conversion_id,
  _fivetran_synced,
  form_id,
  page_url,
  portal_id,
  timestamp,
  title,
FROM
  {{ source('fivetran_hubspot', 'contact_form_submission') }}
