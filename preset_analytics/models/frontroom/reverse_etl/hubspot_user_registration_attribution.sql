{{
  config(
    alias='hubspot_user_registration_attribution',
    materialized='table'
  )
}}

WITH SRA AS (
    SELECT
        combined_blended_user_id,
        email,
        blended_user_id,
        reg_event_user_id,
        reg_event_anon_id,
        identify_user_id,
        identify_anonymous_id,
        session_start_tstamp,
        session_number,
        page_views,
        combined_referrer_source,
        combined_referrer_medium,
        referrer_source,
        referrer_medium,
        utm_source,
        utm_content,
        utm_medium,
        utm_campaign,
        utm_term,
        reg_timestamp,
        reg_first_ab_test,
        reg_event_id,
        registration_type,
        combined_referrer_medium != 'search' AS is_combined_referrer_medium_not_search
    FROM {{ ref( 'wrk_segment_registration_attribution' ) }}
    WHERE LOWER(email) NOT LIKE '%@mattermade%'
        AND (
            session_number = 1
            OR combined_referrer_medium != 'search'
        )
),

MSRA AS (
    SELECT
        combined_blended_user_id,
        LOGICAL_OR(is_combined_referrer_medium_not_search) AS is_combined_referrer_medium_not_search_for_cbui
    FROM SRA
    GROUP BY combined_blended_user_id
)

SELECT
    SRA.combined_blended_user_id,
    SRA.email,
    SRA.session_number,
    SRA.page_views,
    SRA.combined_referrer_source,
    SRA.combined_referrer_medium,
    SRA.referrer_source,
    SRA.referrer_medium,
    SRA.utm_source,
    SRA.utm_content,
    SRA.utm_medium,
    SRA.utm_campaign,
    SRA.utm_term,
    SRA.reg_timestamp,
    SRA.reg_first_ab_test,
    SRA.reg_event_id,
    SRA.registration_type
FROM SRA
INNER JOIN MSRA
    ON SRA.combined_blended_user_id = MSRA.combined_blended_user_id
        AND SRA.is_combined_referrer_medium_not_search = MSRA.is_combined_referrer_medium_not_search_for_cbui
