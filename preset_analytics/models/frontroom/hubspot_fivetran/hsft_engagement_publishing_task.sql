{{
  config(
    alias='engagement_publishing_task',
    materialized='view'
  )
}}

SELECT
  engagement_id,
  _fivetran_synced,
  body,
  campaign_guid,
  category,
  category_id,
  content_id,
  name,
  state,
FROM
  {{ source('fivetran_hubspot', 'engagement_publishing_task') }}
