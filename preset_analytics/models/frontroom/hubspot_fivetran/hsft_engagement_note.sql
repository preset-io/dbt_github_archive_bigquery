{{
  config(
    alias='engagement_note',
    materialized='view'
  )
}}

SELECT
  engagement_id,
  _fivetran_synced,
  body,
FROM
  {{ source('fivetran_hubspot', 'engagement_note') }}
