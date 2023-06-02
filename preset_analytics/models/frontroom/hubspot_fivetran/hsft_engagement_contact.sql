{{
  config(
    alias='engagement_contact',
    materialized='view'
  )
}}

SELECT
  contact_id,
  engagement_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'engagement_contact') }}
