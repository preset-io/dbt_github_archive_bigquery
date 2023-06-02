{{
  config(
    alias='engagement_deal',
    materialized='view'
  )
}}

SELECT
  deal_id,
  engagement_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'engagement_deal') }}
