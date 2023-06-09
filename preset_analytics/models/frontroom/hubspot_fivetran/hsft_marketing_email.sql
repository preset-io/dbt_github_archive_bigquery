{{
  config(
    alias='marketing_email',
    materialized='view'
  )
}}

SELECT
  id,
  _fivetran_synced,
  ab,
  ab_hours_to_wait,
  ab_sample_size_default,
  ab_sampling_default,
  ab_status,
  ab_success_metric,
  ab_test_id,
  ab_test_percentage,
  ab_variation,
  absolute_url,
  analytics_page_id,
  analytics_page_type,
  archived,
  author,
  author_at,
  author_email,
  author_name,
  author_user_id,
  blog_email_type,
  campaign,
  campaign_name,
  can_spam_settings_id,
  cloned_from,
  create_page,
  created,
  currently_published,
  domain,
  email_body,
  email_note,
  email_type,
  feedback_email_category,
  feedback_survey_id,
  folder_id,
  freeze_date,
  from_name,
  is_graymail_suppression_enabled,
  is_local_timezone_send,
  is_published,
  is_recipient_fatigue_suppression_enabled,
  lead_flow_id,
  live_domain,
  meta_description,
  name,
  page_expiry_date,
  page_expiry_redirect_id,
  page_redirected,
  portal_id,
  preview_key,
  processing_status,
  publish_date,
  publish_immediately,
  published_at,
  published_by_id,
  published_by_name,
  published_url,
  reply_to,
  resolved_domain,
  slug,
  subcategory,
  subject,
  subscription,
  subscription_blog_id,
  subscription_name,
  transactional,
  unpublished_at,
  updated,
  updated_by_id,
  url,
FROM
  {{ source('fivetran_hubspot', 'marketing_email') }}
