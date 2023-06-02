{{
  config(
    alias='deal_pipeline',
    materialized='view'
  )
}}

SELECT
  pipeline_id,
  _fivetran_deleted,
  _fivetran_synced,
  active,
  display_order,
  label,
FROM
  {{ source('fivetran_hubspot', 'deal_pipeline') }}
