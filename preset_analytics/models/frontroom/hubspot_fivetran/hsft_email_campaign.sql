{{
  config(
    alias='email_campaign',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  app_id,
  app_name,
  content_id,
  name,
  num_included,
  num_queued,
  sub_type,
  subject,
  type,
FROM
  {{ source('fivetran_hubspot', 'email_campaign') }}
