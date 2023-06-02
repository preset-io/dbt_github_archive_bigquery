{{
  config(
    alias='property',
    materialized='view'
  )
}}

SELECT
  _fivetran_id,
  _fivetran_synced,
  calculated,
  description,
  field_type,
  group_name,
  hubspot_defined,
  hubspot_object,
  label,
  name,
  type,
FROM
  {{ source('fivetran_hubspot', 'property') }}
