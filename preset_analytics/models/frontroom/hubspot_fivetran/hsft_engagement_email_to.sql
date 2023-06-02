{{
  config(
    alias='engagement_email_to',
    materialized='view'
  )
}}

SELECT
  engagement_id,
  _fivetran_synced,
  email,
  first_name,
  last_name,
FROM
  {{ source('fivetran_hubspot', 'engagement_email_to') }}
