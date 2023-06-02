{{
  config(
    alias='company_property_history',
    materialized='view'
  )
}}
/*
 this needs work on proper casting of the timestamp column
 */

SELECT
  _fivetran_start,
  company_id,
  name,
  _fivetran_active,
  _fivetran_end,
  _fivetran_synced,
  source,
  source_id,
  timestamp,
  value,
FROM
  {{ source('fivetran_hubspot', 'company_property_history') }}
