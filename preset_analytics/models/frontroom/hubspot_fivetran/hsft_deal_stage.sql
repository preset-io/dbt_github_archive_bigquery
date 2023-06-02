{{
  config(
    alias='deal_stage',
    materialized='view'
  )
}}

SELECT
  _fivetran_start,
  deal_id,
  _fivetran_active,
  _fivetran_end,
  _fivetran_synced,
  date_entered,
  source,
  source_id,
  value,
FROM
  {{ source('fivetran_hubspot', 'deal_stage') }}
