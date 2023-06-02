{{
  config(
    alias='product_property_history',
    materialized='view'
  )
}}

/*
 this needs some work on the csting of the timestamp column
*/

SELECT
  _fivetran_start,
  name,
  product_id,
  _fivetran_active,
  _fivetran_end,
  _fivetran_synced,
  timestamp,
  value,
FROM
  {{ source('fivetran_hubspot', 'product_property_history') }}
