{{
  config(
    alias='wrk_user_utm_attribution'
  )
}}

WITH SWSS AS (
  SELECT DISTINCT
    SW.blended_user_id,
    MU.created_dttm as preset_user_created_dttm,
    DATETIME(SW.session_start_tstamp) as session_start_dttm,
    SW.session_number,
    SW.referrer_source,
    SW.referrer_medium,
    SW.referrer,
    SW.utm_source,
    SW.utm_medium,
    SW.utm_campaign,
    SW.utm_content,
    SW.utm_term,
    COALESCE(SW.utm_source, CASE WHEN SW.referrer_source IS NULL AND (SW.first_page_url LIKE '%invitation%' OR SW.referrer LIKE '%invitation%') THEN 'invite'
                              WHEN SW.referrer_source IS NULL AND SW.referrer = 'https://docs.preset.io/' THEN 'preset_docs'
                              WHEN SW.referrer_source IS NULL AND SW.referrer IS NULL AND SW.first_page_url LIKE '%blog%' THEN 'preset_blog'
                              WHEN SW.referrer_source IS NULL AND SW.referrer NOT LIKE '%preset.io%' THEN referrer
                              WHEN SW.referrer_source IS NULL AND SW.referrer IS NULL AND SW.first_page_url LIKE 'https://preset.io/%' THEN 'direct'
                              WHEN SW.referrer_source IS NULL AND (SW.referrer LIKE 'https://preset.io/%' OR SW.referrer LIKE 'https://www.preset.io/%') THEN 'direct'
                              WHEN SW.referrer_source IS NULL AND (SW.first_page_url LIKE '%manage.app.preset.io%' OR SW.referrer LIKE 'https://manage.app.preset.io/%') THEN 'direct'
                              WHEN SW.referrer_source IS NULL AND SW.referrer IS NULL THEN 'direct'
                              ELSE SW.referrer_source END) as combined_referrer_source,
    COALESCE(utm_medium, CASE WHEN SW.referrer_medium IS NULL AND (SW.first_page_url LIKE '%invitation%' OR SW.referrer LIKE '%invitation%') THEN 'invite'
                              WHEN SW.referrer_medium IS NULL AND SW.referrer = 'https://docs.preset.io/' THEN 'direct'
                              WHEN SW.referrer_medium IS NULL AND SW.referrer IS NULL AND first_page_url LIKE '%blog%' THEN 'direct'
                              WHEN SW.referrer_medium IS NULL AND SW.referrer NOT LIKE '%preset.io%' THEN 'organic'
                              WHEN SW.referrer_medium IS NULL AND SW.referrer IS NULL AND SW.first_page_url LIKE 'https://preset.io/%' THEN 'direct'
                              WHEN SW.referrer_medium IS NULL AND (SW.referrer LIKE 'https://preset.io/%' OR SW.referrer LIKE 'https://www.preset.io/%') THEN 'direct'
                              WHEN SW.referrer_medium IS NULL AND (SW.first_page_url LIKE '%manage.app.preset.io%' OR SW.referrer LIKE 'https://manage.app.preset.io/%') THEN 'direct'
                              WHEN SW.referrer_medium IS NULL AND SW.referrer IS NULL THEN 'direct'
                              ELSE SW.referrer_medium END) as combined_referrer_medium,
    CASE WHEN COALESCE(SW.utm_medium, SW.referrer_medium) IN ('sponcon','paidsearch','display','CPC', 'ppc') THEN 1
         WHEN COALESCE(SW.utm_medium, SW.referrer_medium) IN ('social', 'rss', 'email') THEN 2
         WHEN COALESCE(SW.utm_medium, SW.referrer_medium) IN ('search') THEN 3
         WHEN COALESCE(SW.utm_medium, SW.referrer_medium) IS NOT NULL THEN 4
         ELSE 5
         END as combined_referrer_medium_priority,

  FROM
    {{ ref( 'segment_web_sessions' ) }} as SW
  LEFT JOIN
    {{ ref( 'manager_preset_user_latest' ) }} as MU
  ON SW.blended_user_id = SAFE_CAST(MU.user_id AS STRING)
  ),

SWS AS (
  SELECT
    SWSS.*
  FROM
    SWSS
  WHERE
    SWSS.preset_user_created_dttm >= SWSS.session_start_dttm
    OR SWSS.preset_user_created_dttm IS NULL
  ),

MSWS AS (
  SELECT DISTINCT
    SWS.*,
    MIN(SWS.combined_referrer_medium_priority) OVER (PARTITION BY SWS.blended_user_id) as min_combined_referrer_medium_priority
  FROM
    SWS
  ),

MNSWS AS (
  SELECT DISTINCT
    MSWS.*,
    MIN(MSWS.session_number) OVER (PARTITION BY MSWS.blended_user_id) as min_session_number
  FROM
    MSWS
  WHERE
    MSWS.combined_referrer_medium_priority = MSWS.min_combined_referrer_medium_priority
)

SELECT DISTINCT
  MNSWS.*,
  CASE WHEN LOWER(MNSWS.combined_referrer_medium) IN ('cpc', 'ppc', 'paidsearch', 'paid search') THEN 'Paid Search'
         WHEN LOWER(MNSWS.combined_referrer_medium) IN ('display', '+cta+start+free+today', 'sponcon', 'social-paid', 'fbdvby') THEN 'Display'
         WHEN LOWER(MNSWS.combined_referrer_medium) IN ('email', 'nurture') or LOWER(MNSWS.combined_referrer_source) IN ('email') THEN 'Email'
         WHEN LOWER(MNSWS.combined_referrer_medium) IN ('referral') THEN 'Referral'
         WHEN LOWER(MNSWS.combined_referrer_medium) IN ('social', 'rss') THEN 'Social'
         WHEN LOWER(MNSWS.combined_referrer_medium) IN ('organic', 'search') OR (LOWER(MNSWS.combined_referrer_medium) = 'unknown' AND LOWER(MNSWS.combined_referrer_source) IN ('google', 'yahoo!')) THEN 'Organic Search'
         WHEN LOWER(MNSWS.combined_referrer_medium) IN ('direct', '', 'invite') OR MNSWS.combined_referrer_medium IS NULL THEN 'Direct'
         ELSE '(Other)'
         END as channel_grouping
FROM
  MNSWS
WHERE
  MNSWS.session_number = MNSWS.min_session_number
