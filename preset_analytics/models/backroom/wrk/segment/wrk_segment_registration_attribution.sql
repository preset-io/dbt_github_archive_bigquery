{{
  config(
    alias='wrk_segment_registration_attribution',
    materialized='table'
  )
}}

WITH SPRG AS (
    SELECT
        COALESCE(user_id, anonymous_id) as blended_user_id,
        anonymous_id,
        context_library_name,
        context_library_version,
        event,
        event_text,
        first_ab_test,
        id,
        loaded_at,
        original_timestamp,
        received_at,
        referrer,
        sent_at,
        timestamp,
        user_id,
        useragent AS user_agent,
        uuid_ts,
        ip_address AS ip,
        'professional' AS registration_type
    FROM {{ source( 'manager_events_production', 'complete_registration_professional' ) }}

    UNION ALL

    SELECT
        COALESCE(user_id, anonymous_id) as blended_user_id,
        anonymous_id,
        context_library_name,
        context_library_version,
        event,
        event_text,
        first_ab_test,
        id,
        loaded_at,
        original_timestamp,
        received_at,
        referrer,
        sent_at,
        timestamp,
        user_id,
        useragent AS user_agent,
        uuid_ts,
        ip_address AS ip,
        'starter' AS registration_type
    FROM {{ source( 'manager_events_production', 'complete_registration_starter' ) }}
)

SELECT DISTINCT
    MU.email,
    SWS.blended_user_id,
    SPRG.user_id AS reg_event_user_id,
    SPRG.anonymous_id AS reg_event_anon_id,
    I.user_id AS identify_user_id,
    I.anonymous_id AS identify_anonymous_id,
    SWS.session_start_tstamp,
    SWS.session_number,
    SWS.page_views,
    SWS.referrer_source,
    SWS.referrer_medium,
    SWS.utm_source,
    SWS.utm_content,
    SWS.utm_medium,
    SWS.utm_campaign,
    SWS.utm_term,
    SPRG.original_timestamp AS reg_timestamp,
    SPRG.first_ab_test AS reg_first_ab_test,
    SPRG.id AS reg_event_id,
    SPRG.registration_type,
    COALESCE(I.user_id, SWS.blended_user_id, SPRG.user_id, SPRG.anonymous_id) AS combined_blended_user_id,
    COALESCE(SWS.utm_source, SWS.referrer_source) AS combined_referrer_source,
    COALESCE(SWS.utm_medium, SWS.referrer_medium) AS combined_referrer_medium
FROM SPRG
LEFT JOIN {{ ref( 'segment_web_sessions' ) }} AS SWS
    ON SPRG.blended_user_id = SWS.blended_user_id
LEFT JOIN {{ source( 'manager_events_production', 'identifies' ) }} AS I
    ON I.user_id IS NOT NULL
        AND I.anonymous_id IS NOT NULL
        AND (
            (I.user_id = SPRG.user_id AND I.anonymous_id = SPRG.anonymous_id)
            OR I.anonymous_id = SPRG.anonymous_id
            OR I.user_id = SPRG.user_id
        )
LEFT JOIN {{ ref( 'manager_preset_user_latest' ) }} AS MU
    ON SAFE_CAST(MU.user_id AS STRING) = SPRG.user_id
        OR SAFE_CAST(MU.user_id AS STRING) = I.user_id
WHERE SWS.referrer_source IS NOT NULL
    OR SWS.referrer_medium IS NOT NULL
    OR SWS.utm_source IS NOT NULL
    OR SWS.utm_content IS NOT NULL
    OR SWS.utm_medium IS NOT NULL
    OR SWS.utm_campaign IS NOT NULL
    OR SWS.utm_term IS NOT NULL
