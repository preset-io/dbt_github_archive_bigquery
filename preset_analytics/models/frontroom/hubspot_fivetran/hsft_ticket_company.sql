{{
  config(
    alias='ticket_company',
    materialized='view'
  )
}}

SELECT
  company_id,
  ticket_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'ticket_company') }}
