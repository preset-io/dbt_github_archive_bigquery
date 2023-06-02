{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'}
) }}

WITH date_spine AS (
    SELECT
        dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this) }}
    {% endif %}
)

, user_mapped_views AS (
    SELECT DISTINCT
        COALESCE(users.user_id, views.anonymous_id) AS blended_user_id,
        views.*,
    FROM {{ ref('wrk_segment_unified_page_views') }} AS views
    LEFT JOIN {{ ref('segment_web_user_stitching') }} AS users
        ON views.anonymous_id = users.anonymous_id
)

, segment_events_first_user_activity AS (
    SELECT
        blended_user_id,
        MIN(DATE(original_timestamp)) AS first_event_dt, --includes manager events
        MIN(CASE event WHEN 'website_page' THEN DATE(original_timestamp) END) AS first_website_visit_dt, --only marketing website events
    FROM user_mapped_views
    GROUP BY blended_user_id
)

, segment_events_summary AS (
    SELECT
        DATE(views.original_timestamp) AS dt,
        COUNT(views.id) AS num_page_views,
        COUNT(DISTINCT views.blended_user_id) AS num_distinct_page_viewing_users,
        COUNT(DISTINCT CASE WHEN DATE(views.original_timestamp) = user_events.first_website_visit_dt THEN views.blended_user_id END) AS num_first_website_visits,
        COUNT(CASE WHEN LOWER(views.url) LIKE '%registration%' OR LOWER(views.event_text) LIKE '%registration%' THEN views.blended_user_id END) AS num_registration_page_views,
        COUNT(DISTINCT CASE WHEN LOWER(views.url) LIKE '%registration%' OR LOWER(views.event_text) LIKE '%registration%' THEN views.blended_user_id END) AS num_distinct_registration_page_viewing_users,
        COUNT(CASE WHEN LOWER(views.url) LIKE '%contact-sales%' THEN views.blended_user_id END) AS num_contact_sales_page_views,
        COUNT(DISTINCT CASE WHEN LOWER(views.url) LIKE '%contact-sales%' THEN views.blended_user_id END) AS num_distinct_contact_sales_page_viewing_users,
    FROM user_mapped_views AS views
    LEFT JOIN segment_events_first_user_activity AS user_events
        ON views.blended_user_id = user_events.blended_user_id
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this, date_col='DATE(views.original_timestamp)') }}
    {% endif %}
    GROUP BY DATE(views.original_timestamp)
)

SELECT
    date_spine.dt,
    segment_events_summary.num_page_views,
    segment_events_summary.num_distinct_page_viewing_users,
    segment_events_summary.num_first_website_visits,
    segment_events_summary.num_registration_page_views,
    segment_events_summary.num_distinct_registration_page_viewing_users,
    segment_events_summary.num_contact_sales_page_views,
    segment_events_summary.num_distinct_contact_sales_page_viewing_users,
FROM date_spine
LEFT JOIN segment_events_summary
    ON segment_events_summary.dt = date_spine.dt
WHERE date_spine.dt > '2022-02-03' -- site events do not seem to be collected before this date
