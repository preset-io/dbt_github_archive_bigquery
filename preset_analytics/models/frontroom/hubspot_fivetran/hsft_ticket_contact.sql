{{
  config(
    alias='ticket_contact',
    materialized='view'
  )
}}

SELECT
  contact_id,
  ticket_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'ticket_contact') }}
