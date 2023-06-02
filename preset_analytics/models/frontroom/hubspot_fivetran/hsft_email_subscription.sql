{{
  config(
    alias='email_subscription',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  active,
  description,
  name,
  portal_id,
FROM
  {{ source('fivetran_hubspot', 'email_subscription') }}
