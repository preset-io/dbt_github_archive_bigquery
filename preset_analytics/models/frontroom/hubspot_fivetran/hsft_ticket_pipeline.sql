{{
  config(
    alias='ticket_pipeline',
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
  object_type_id,
FROM
  {{ source('fivetran_hubspot', 'ticket_pipeline') }}
