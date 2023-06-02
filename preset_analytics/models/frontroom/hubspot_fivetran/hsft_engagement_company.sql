{{
  config(
    alias='engagement_company',
    materialized='view'
  )
}}

SELECT
  company_id,
  engagement_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'engagement_company') }}
