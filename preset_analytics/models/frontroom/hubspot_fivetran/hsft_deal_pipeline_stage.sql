{{
  config(
    alias='deal_pipeline_stage',
    materialized='view'
  )
}}

SELECT
  stage_id,
  _fivetran_deleted,
  _fivetran_synced,
  active,
  closed_won,
  display_order,
  label,
  pipeline_id,
  probability,
FROM
  {{ source('fivetran_hubspot', 'deal_pipeline_stage') }}
