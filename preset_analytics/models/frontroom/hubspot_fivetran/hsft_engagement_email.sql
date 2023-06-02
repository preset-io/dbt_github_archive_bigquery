{{
  config(
    alias='engagement_email',
    materialized='view'
  )
}}

SELECT
  engagement_id,
  _fivetran_synced,
  attached_video_id,
  attached_video_opened,
  attached_video_watched,
  email_send_event_id_created,
  email_send_event_id_id,
  error_message,
  facsimile_send_id,
  from_email,
  from_first_name,
  from_last_name,
  html,
  logged_from,
  media_processing_status,
  member_of_forwarded_subthread,
  message_id,
  pending_inline_image_ids,
  post_send_status,
  recipient_drop_reasons,
  sent_via,
  status,
  subject,
  text,
  thread_id,
  tracker_key,
  validation_skipped,
FROM
  {{ source('fivetran_hubspot', 'engagement_email') }}
