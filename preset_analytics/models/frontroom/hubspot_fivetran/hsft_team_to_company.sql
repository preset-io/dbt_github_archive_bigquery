{{
  config(
    alias='team_to_company',
    materialized='view'
  )
}}

SELECT
  from_id,
  to_id,
  _fivetran_synced,
  type,
FROM
  {{ source('fivetran_hubspot', 'team_to_company') }}
