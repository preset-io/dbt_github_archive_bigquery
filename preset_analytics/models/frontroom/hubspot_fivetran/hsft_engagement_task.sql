{{
  config(
    alias='engagement_task',
    materialized='view'
  )
}}

SELECT
  engagement_id,
  _fivetran_synced,
  body,
  completion_date,
  for_object_type,
  is_all_day,
  priority,
  probability_to_complete,
  status,
  subject,
  task_type,
FROM
  {{ source('fivetran_hubspot', 'engagement_task') }}
