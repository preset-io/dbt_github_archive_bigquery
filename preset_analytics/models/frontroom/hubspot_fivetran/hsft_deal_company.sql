{{
  config(
    alias='deal_company',
    materialized='view'
  )
}}

SELECT
  company_id,
  deal_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'deal_company') }}
