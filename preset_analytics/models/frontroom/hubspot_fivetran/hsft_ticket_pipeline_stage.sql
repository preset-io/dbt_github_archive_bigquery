{{
  config(
    alias='ticket_pipeline_stage',
    materialized='view'
  )
}}

SELECT
  stage_id,
  _fivetran_deleted,
  _fivetran_synced,
  active,
  display_order,
  is_closed,
  label,
  pipeline_id,
  ticket_state,
FROM
  {{ source('fivetran_hubspot', 'ticket_pipeline_stage') }}
