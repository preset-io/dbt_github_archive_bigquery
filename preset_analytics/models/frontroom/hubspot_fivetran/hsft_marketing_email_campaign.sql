{{
  config(
    alias='marketing_email_campaign',
    materialized='view'
  )
}}

SELECT
  campaign_id,
  marketing_email_id,
  _fivetran_synced,
FROM
  {{ source('fivetran_hubspot', 'marketing_email_campaign') }}
