{{
  config(
    alias='ticket_engagement',
    materialized='view'
  )
}}

SELECT
  engagement_id,
  ticket_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'ticket_engagement') }}
