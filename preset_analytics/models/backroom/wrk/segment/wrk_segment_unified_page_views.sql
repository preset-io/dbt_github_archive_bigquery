{#
NOTE: "full_refresh = ..." has to do with the fact that the
source data can disapear over time, if/when Segment changes the retention
period
#}

{{ warn_on_full_refresh_attempt(this) }}

{{
  config(
    alias='wrk_segment_unified_page_views',
    materialized='incremental',
    full_refresh = var("super_full_refresh_use_with_care"),
    unique_key='id'
  )
}}

WITH backfill AS (
  SELECT * FROM {{ source('seeds', 'seed_wrk_segment_unified_page_views') }}
),

mgr_pages AS (

    SELECT DISTINCT
      anonymous_id,
      context_library_name,
      context_library_version,
      'manager_page' AS event,
      COALESCE(context_page_title, name) AS event_text,
      fingerprint,
      first_ab_test,
      CONCAT(id, '-', STRING(uuid_ts)) AS id,
      context_ip,
      context_locale,
      loaded_at,
      original_timestamp,
      context_page_path AS path,
      received_at,
      COALESCE(context_page_referrer, referrer) AS referrer,
      context_page_search AS search,
      sent_at,
      team_id,
      timestamp,
      COALESCE(context_page_title, name) AS title,
      COALESCE(context_page_url, referrer) AS url,
      COALESCE(context_user_agent, useragent) AS context_user_agent,
      user_id,
      uuid_ts
    FROM
        {{ source( 'manager_events_production', 'pages' ) }}
),

context_campaign_extract_logic AS (
    SELECT
      *,
      {% set context_campaign_fields = [{"field_name": "source", "search_string":"utm_source"},
                                        {"field_name": "medium", "search_string":"utm_medium"},
                                        {"field_name": "name", "search_string":"utm_campaign"},
                                        {"field_name": "content", "search_string":"utm_content"},
                                        {"field_name": "term", "search_string":"utm_term"}]
      %}

      {% for field in context_campaign_fields %}
        CASE WHEN url LIKE '%{{field['search_string']}}%' THEN SPLIT(SPLIT(url, '{{field['search_string']}}=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)]
          WHEN referrer LIKE '%{{field['search_string']}}%' THEN SPLIT(SPLIT(referrer, '{{field['search_string']}}=')[SAFE_OFFSET(1)], '&')[SAFE_OFFSET(0)]
          ELSE NULL
          END AS context_campaign_{{field['field_name']}},
      {% endfor %}
    FROM mgr_pages
),

www_pages AS (
    SELECT DISTINCT
      anonymous_id,
      context_library_name,
      context_library_version,
      'website_page' AS event,
      COALESCE(context_page_title, title) AS event_text,
      fingerprint,
      SAFE_CAST(NULL AS STRING) AS first_ab_test,
      CONCAT(id, '-', STRING(uuid_ts)) as id,
      context_ip,
      context_locale,
      loaded_at,
      original_timestamp,
      COALESCE(context_page_path, CONCAT('/', REGEXP_EXTRACT(url, r'(?:[a-zA-Z]+://)?(?:[a-zA-Z0-9-.]+)/{1}([a-zA-Z0-9-./]+)')), '/') AS path,
      received_at,
      COALESCE(context_page_referrer, referrer) AS referrer,
      COALESCE(context_page_search, REGEXP_EXTRACT(url, r'\?(.*)')) AS search,
      sent_at,
      SAFE_CAST(NULL AS INT64) AS team_id,
      timestamp,
      COALESCE(context_page_title, title) AS title,
      COALESCE(context_page_url, url) AS url,
      context_user_agent,
      SAFE_CAST(NULL AS STRING) AS user_id,
      uuid_ts,
      context_campaign_source,
      context_campaign_medium,
      context_campaign_name,
      context_campaign_content,
      context_campaign_term,
    FROM
        {{ source( 'production_gatsby_marketing_website', 'pages' ) }}
),

preset_client_side_app_pages AS (
    SELECT DISTINCT
      anonymous_id,
      context_library_name,
      context_library_version,
      'preset_app_page' AS event,
      COALESCE(name, context_page_title) AS event_text,
      SAFE_CAST(NULL AS STRING) AS fingerprint,
      SAFE_CAST(NULL AS STRING) AS first_ab_test,
      CONCAT(id, '-', STRING(uuid_ts)) as id,
      context_ip,
      context_locale,
      loaded_at,
      original_timestamp,
      COALESCE(context_page_path, CONCAT('/', REGEXP_EXTRACT(url, r'(?:[a-zA-Z]+://)?(?:[a-zA-Z0-9-.]+)/{1}([a-zA-Z0-9-./]+)')), '/') AS path,
      received_at,
      COALESCE(context_page_referrer, referrer) AS referrer,
      COALESCE(context_page_search, REGEXP_EXTRACT(url, r'\?(.*)')) AS search,
      sent_at,
      SAFE_CAST(NULL AS INT64) AS team_id,
      timestamp,
      COALESCE(context_page_title, title) AS title,
      COALESCE(context_page_url, url) AS url,
      context_user_agent,
      SAFE_CAST(user_id AS STRING) as user_id,
      uuid_ts,
      context_campaign_source,
      context_campaign_medium,
      context_campaign_name,
      context_campaign_content,
      context_campaign_term,
  FROM
    {{ source( 'preset_app_client_side_prod', 'pages' ) }}
),

all_pages AS (
    SELECT * FROM context_campaign_extract_logic

    UNION ALL

    SELECT * FROM www_pages

    UNION ALL

    SELECT * FROM preset_client_side_app_pages
),

all_pages_w_backfill AS (
  SELECT * FROM all_pages

  UNION DISTINCT

  SELECT * FROM backfill
)

SELECT
    anonymous_id,
    context_library_name,
    context_library_version,
    event,
    event_text,
    fingerprint,
    first_ab_test,
    id,
    context_ip,
    context_locale,
    loaded_at,
    original_timestamp,
    path,
    received_at,
    referrer,
    search,
    sent_at,
    team_id,
    timestamp,
    title,
    url,
    context_user_agent,
    user_id,
    uuid_ts,
    context_campaign_source,
    context_campaign_medium,
    context_campaign_name,
    context_campaign_content,
    context_campaign_term,
FROM
    all_pages_w_backfill

{% if is_incremental() %}

    WHERE {{ generate_incremental_statement(this, date_col='DATE(received_at)', this_date_col='received_at') }}

{% endif %}
