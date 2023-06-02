{{
  config(
    alias='line_item_property_history',
    materialized='view'
  )
}}

/*
 this needs some work on the csting of the timestamp column
*/

SELECT
  _fivetran_start,
  line_item_id,
  name,
  _fivetran_active,
  _fivetran_end,
  _fivetran_synced,
  selected,
  timestamp,
  value,
FROM
  {{ source('fivetran_hubspot', 'line_item_property_history') }}
