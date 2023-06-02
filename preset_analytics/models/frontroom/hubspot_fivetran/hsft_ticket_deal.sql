{{
  config(
    alias='ticket_deal',
    materialized='view'
  )
}}

SELECT
  deal_id,
  ticket_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'ticket_deal') }}
