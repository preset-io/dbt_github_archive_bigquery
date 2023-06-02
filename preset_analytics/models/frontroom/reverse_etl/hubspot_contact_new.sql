WITH base AS (
    SELECT
        raw.email,
        MIN(mu.user_id) AS preset_user_id,
        MAX(SPLIT(mu.email_domain, ".")[OFFSET(0)]) AS preset_company,
        MAX(mu.is_preset) AS is_preset,
        MAX(mu.active) AS active,
        UNIX_MILLIS(MAX(CAST(DATE(mu.last_login) AS TIMESTAMP))) AS last_login,
        SUM(mu.login_count) AS login_count,
        SUM(mu.fail_login_count) AS fail_login_count,
        MIN(mu.created_dttm) AS created_dttm,
        SUM(mu.dashboards_created_count) AS dashboards_created_count,
        SUM(mu.non_example_dashboards_created_count) AS non_example_dashboards_created_count,
        SUM(mu.dashboards_ltd_views) AS dashboards_ltd_views,
        SUM(mu.dashboards_n28d_views) AS dashboards_n28d_views,
        SUM(mu.dashboards_ltd_users) AS dashboards_ltd_users,
        SUM(mu.dashboards_n28d_users) AS dashboards_n28d_users,
        MAX(mu.has_dashboard_viewed_by_others) AS has_dashboard_viewed_by_others,
        SUM(mu.charts_created_count) AS charts_created_count,
        SUM(mu.non_example_charts_created_count) AS non_example_charts_created_count,
        TO_JSON_STRING(ARRAY_CONCAT_AGG(mu.connected_database_type_array)) AS connected_database_type_array,
        SUM(mu.invites_sent) AS invites_sent,
        SUM(mu.invites_pending) AS invites_pending,
        SUM(mu.invites_accepted) AS invites_accepted,
        MAX(mu.l7) AS l7,
        MAX(mu.l28) AS l28,
        SUM(mu.ltd_visits) AS ltd_visits,
        TO_JSON_STRING(ARRAY_CONCAT_AGG(mu.team_id_array)) AS team_ids,
        TO_JSON_STRING(ARRAY_CONCAT_AGG(mu.team_role_array)) AS team_roles,
        MAX(mu.highest_billing_status) AS highest_billing_status,
        MAX(mu.is_team_creator) AS is_team_creator,
        MAX(mu.has_superset_experience) AS has_superset_experience,
        MAX(mu.department) AS department,
        MAX(mu.role) AS role,
        CASE MAX(CASE mu.company_size
          WHEN "SELF_EMPLOYED" THEN 1
          WHEN "EMPLOYEES_2_5" THEN 2
          WHEN "EMPLOYEES_6_100" THEN 3
          WHEN "EMPLOYEES_101_1000" THEN 4
          WHEN "EMPLOYEES_1001_10000" THEN 5
          WHEN "EMPLOYEES_10001_PLUS" THEN 6
        END)
          WHEN 1 THEN "SELF_EMPLOYED"
          WHEN 2 THEN "EMPLOYEES_2_5"
          WHEN 3 THEN "EMPLOYEES_6_100"
          WHEN 4 THEN "EMPLOYEES_101_1000"
          WHEN 5 THEN "EMPLOYEES_1001_10000"
          WHEN 6 THEN "EMPLOYEES_10001_PLUS"
        END AS company_size,
    FROM {{ ref('manager_preset_user_latest') }} AS raw
    LEFT JOIN {{ ref('manager_user') }} AS mu
      ON raw.user_id = mu.user_id
    WHERE mu.hs_contact_id IS NULL
    GROUP BY 1
)



SELECT
    base.email,
    base.preset_user_id,
    base.is_preset,
    base.active,
    base.last_login,
    base.login_count,
    base.fail_login_count,
    base.created_dttm,
    user.first_name,
    user.last_name,
    -- logic mimics what had been in the manager2hubspot sync
    COALESCE(
      base.preset_company,
      CASE ARRAY_LENGTH(SPLIT(user.email, '@'))
        WHEN 2 THEN SPLIT(user.email, '@')[OFFSET(1)]
      END
    ) AS preset_company,
    CASE WHEN user.email_marketing THEN "Yes" ELSE "No" END AS preset_user_email_opt_in,
    base.dashboards_created_count,
    base.non_example_dashboards_created_count,
    base.dashboards_ltd_views,
    base.dashboards_n28d_views,
    base.dashboards_ltd_users,
    base.dashboards_n28d_users,
    base.has_dashboard_viewed_by_others,
    base.charts_created_count,
    base.non_example_charts_created_count,
    base.connected_database_type_array,
    base.invites_sent,
    base.invites_sent AS preset___of_invites_sent,
    base.invites_pending,
    base.invites_accepted,
    base.l7,
    base.l28,
    base.ltd_visits,
    base.team_ids AS preset_team_ids,
    base.team_roles AS preset_team_roles,
    base.highest_billing_status,
    CASE base.highest_billing_status
      WHEN "ENTERPRISE" THEN "Enterprise"
      WHEN "FREE" THEN "Starter"
      ELSE "Professional"
    END AS preset_tier,
    base.is_team_creator AS preset_team_creator,
    CASE WHEN base.has_superset_experience THEN "Yes" ELSE "No" END AS has_superset_experience,
    base.department,
    base.role,
    base.company_size,
    events.team_hash_first,
    events.team_hash_latest,
    events.team_hash_most_active,
    events.workspace_hostname_first,
    events.workspace_hostname_latest,
    events.workspace_hostname_is_most_active,
    UUA.utm_source,
    UUA.utm_medium,
    UUA.utm_campaign,
    UUA.utm_content,
    UUA.utm_term,
    UUA.combined_referrer_source,
    UUA.combined_referrer_medium,
    UUA.channel_grouping
FROM base
LEFT JOIN {{ ref('user_events_agg_denorm') }} AS events
  ON SAFE_CAST(base.preset_user_id AS STRING) = events.user_id
LEFT JOIN {{ ref('wrk_user_utm_attribution') }} AS UUA
  ON SAFE_CAST(base.preset_user_id AS STRING) = UUA.blended_user_id
LEFT JOIN {{ ref('manager_preset_user_latest') }} AS user
  ON base.preset_user_id = user.user_id
