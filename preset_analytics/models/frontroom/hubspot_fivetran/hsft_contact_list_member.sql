{{
  config(
    alias='contact_list_member',
    materialized='view'
  )
}}

SELECT
  contact_id,
  contact_list_id,
  _fivetran_deleted,
  _fivetran_synced,
  added_at,
FROM
  {{ source('fivetran_hubspot', 'contact_list_member') }}
