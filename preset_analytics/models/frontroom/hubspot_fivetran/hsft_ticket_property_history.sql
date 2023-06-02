{{
  config(
    alias='ticket_property_history',
    materialized='view'
  )
}}

/*
 this needs some work on the csting of the timestamp column
*/

SELECT
  _fivetran_start,
  name,
  ticket_id,
  _fivetran_active,
  _fivetran_end,
  _fivetran_synced,
  source,
  source_id,
  timestamp_instant,
  value,
FROM
  {{ source('fivetran_hubspot', 'ticket_property_history') }}
