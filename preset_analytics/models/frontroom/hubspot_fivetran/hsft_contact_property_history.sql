{{
  config(
    alias='contact_property_history',
    materialized='view'
  )
}}

/*
 this needs some work on properly casting the timestamp column
*/

SELECT
  _fivetran_start,
  contact_id,
  name,
  _fivetran_active,
  _fivetran_end,
  _fivetran_synced,
  source,
  source_id,
  timestamp,
  value,
FROM
  {{ source('fivetran_hubspot', 'contact_property_history') }}
