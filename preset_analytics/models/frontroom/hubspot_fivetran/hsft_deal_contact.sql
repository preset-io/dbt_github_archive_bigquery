{{
  config(
    alias='deal_contact',
    materialized='view'
  )
}}

SELECT
  contact_id,
  deal_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'deal_contact') }}
