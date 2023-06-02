{{
  config(
    alias='fivetran_audit',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  done,
  message,
  progress,
  rows_updated_or_inserted,
  schema,
  start,
  status,
  table,
  update_id,
  update_started,
FROM
  {{ source('fivetran_hubspot', 'fivetran_audit') }}
