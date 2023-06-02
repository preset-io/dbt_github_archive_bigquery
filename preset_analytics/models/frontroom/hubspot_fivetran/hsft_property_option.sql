{{
  config(
    alias='property_option',
    materialized='view'
  )
}}

SELECT
  label,
  property_id,
  _fivetran_synced,
  display_order,
  double_data,
  hidden,
  read_only,
  value,
FROM
  {{ source('fivetran_hubspot', 'property_option') }}
