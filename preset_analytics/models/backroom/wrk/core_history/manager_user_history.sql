{{ config(
  materialized='incremental',
  incremental_strategy = 'insert_overwrite',
  partition_by = {'field': 'ds', 'data_type': 'date'},
) }}

-- {% set dt = latest_dt(ref('wrk_egaf_events'), 'dt', previous_day=false) %}
{% set results = run_query("SELECT list_id, manager_user_boolean_column_name FROM " ~ ref("seed_hs_list_pivot")) %}



WITH date_spine AS (
  SELECT dt,
  FROM {{ ref('date_spine') }}
  {% if is_incremental() %}
  WHERE {{ generate_incremental_statement(this, this_date_col='ds') }}
  {% endif %}
),

user_dashboard AS (
  SELECT
    ds,
    creator_user_id,
    COUNT(DISTINCT dashboard_key) AS dashboards_created_count,
    COUNT(DISTINCT (CASE WHEN NOT is_example THEN dashboard_key END)) AS non_example_dashboards_created_count,
    SUM(ltd_views) AS dashboards_ltd_views,
    SUM(n28d_views) AS dashboards_n28d_views,
    SUM(ltd_users) AS dashboards_ltd_users,
    SUM(n28d_users) AS dashboards_n28d_users,
  FROM {{ ref('superset_dashboard_history') }}
  GROUP BY 1, 2
),

user_invite AS (
  SELECT
    ds,
    creator_user_id,
    COUNT(*) AS invites_sent,
    COUNT(CASE invite_status WHEN 'PENDING' THEN creator_user_id END) AS invites_pending,
    COUNT(DISTINCT (CASE invite_status WHEN 'ACCEPTED' THEN accepted_by_user_id END)) AS invites_accepted,
  FROM {{ ref('manager_invite_history') }}
  GROUP BY 1, 2
),

user_egaf_events AS (
  SELECT
    date_spine.dt AS ds,
    CAST(egaf.entity_id AS INT) AS entity_id,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(date_spine.dt, egaf.dt, DAY) BETWEEN 0 AND 6 THEN egaf.dt END ) AS l7,
    COUNT(DISTINCT CASE WHEN DATE_DIFF(date_spine.dt, egaf.dt, DAY) BETWEEN 0 AND 27 THEN egaf.dt END ) AS l28,
    COUNT(*) AS ltd_visits,
  FROM date_spine
  LEFT JOIN {{ ref('wrk_egaf_events') }} AS egaf
    ON egaf.dt <= date_spine.dt
  WHERE entity_type = 'user'
    -- records aggregated at the full team level
    AND egaf.workspace_hash = 'ALL WORKSPACES'
  GROUP BY 1, 2
),

user_chart AS (
  SELECT
    ds,
    creator_user_id,
    COUNT(DISTINCT chart_key) AS charts_created_count,
    COUNT(DISTINCT (CASE WHEN NOT is_example THEN chart_key END)) AS non_example_charts_created_count,
    SUM(n90d_views) AS charts_n90d_views,
    SUM(n28d_views) AS charts_n28d_views,
  FROM {{ ref('superset_chart_history') }}
  GROUP BY 1, 2
),

user_database AS (
  SELECT
    ds,
    creator_user_id,
    COUNT(DISTINCT database_connection_key) AS database_connections_created_count,
    COUNT(DISTINCT (CASE WHEN NOT is_example THEN database_connection_key END)) AS non_example_database_connections_created_count,
    ARRAY_AGG(DISTINCT database_driver) AS connected_database_type_array,
  FROM {{ ref('superset_database_connection_history') }}
  GROUP BY 1, 2
),

user_saved_query AS (
  SELECT
    ds,
    creator_user_id,
    COUNT(DISTINCT saved_query_key) AS saved_queries_created_count,
    COUNT(DISTINCT (CASE WHEN NOT is_example THEN saved_query_key END)) AS non_example_saved_queries_created_count,
  FROM {{ ref('superset_saved_query_history') }}
  GROUP BY 1, 2
)

SELECT
  date_spine.dt AS ds,
  A.user_id,
  A.is_preset,
  A.active,
  A.last_login,
  A.login_count,
  A.fail_login_count,
  A.onboarded,
  A.company_name,
  A.company_category,
  A.company_size,
  A.description,
  A.created_dttm,
  A.last_modified_dttm,
  A.creator_user_id,
  A.last_modified_user_id,
  A.email_domain,
  A.hs_contact_id,
  A.hs_company_id,
  A.email_marketing,
  A.team_id_array,
  A.team_role_array,
  A.highest_billing_status,
  A.workspace_id_array,
  A.workspace_role_array,
  A.is_creator,
  A.is_viewer,
  COALESCE(D.dashboards_created_count, 0) AS dashboards_created_count,
  COALESCE(D.non_example_dashboards_created_count, 0) AS non_example_dashboards_created_count,
  COALESCE(D.dashboards_ltd_views, 0) AS dashboards_ltd_views,
  COALESCE(D.dashboards_n28d_views, 0) AS dashboards_n28d_views,
  COALESCE(D.dashboards_ltd_users, 0) AS dashboards_ltd_users,
  COALESCE(D.dashboards_n28d_users, 0) AS dashboards_n28d_users,
  COALESCE(D.dashboards_ltd_users, 0) > 1 AS has_dashboard_viewed_by_others,
  COALESCE(K.charts_created_count, 0) AS charts_created_count,
  COALESCE(K.non_example_charts_created_count, 0) AS non_example_charts_created_count,
  COALESCE(K.charts_n90d_views, 0) AS charts_n90d_views,
  COALESCE(K.charts_n28d_views, 0) AS charts_n28d_views,
  COALESCE(M.database_connections_created_count, 0) AS database_connections_created_count,
  COALESCE(M.non_example_database_connections_created_count, 0) AS non_example_database_connections_created_count,
  M.connected_database_type_array,
  COALESCE(N.saved_queries_created_count, 0) AS saved_queries_created_count,
  COALESCE(N.non_example_saved_queries_created_count, 0) AS non_example_saved_queries_created_count,
  COALESCE(E.invites_sent, 0) AS invites_sent,
  COALESCE(E.invites_pending, 0) AS invites_pending,
  COALESCE(E.invites_accepted, 0) AS invites_accepted,
  COALESCE(F.l7, 0) AS l7,
  COALESCE(F.l28, 0) AS l28,
  COALESCE(F.ltd_visits, 0) AS ltd_visits,

  -- Recurly fields
  A.recurly_arr_paying,
  A.recurly_arr_trial,
  A.recurly_trial_status,
  A.recurly_plan_code,

  -- from manager_user_onboarding
  A.has_superset_experience,
  A.role,
  A.department,
  A.intent_choice_evaluator,
  A.intent_choice_connector,
  A.intent_choice_builder,
  A.intent_choice_consumer,
  A.intent_choice_other,

  A.is_team_creator,
  A.highest_workspace_role,
  hs_list_id_array,
  {% for l in results %}
  {# for each entry in the spreadsheet #}
  {{ l[0] }} IN UNNEST(hs_list_id_array) AS {{ l[1] }},
  {% endfor %}
FROM date_spine
LEFT JOIN {{ ref('wrk_manager_user') }} AS A
  ON date_spine.dt = DATE(A.ds)
LEFT JOIN user_dashboard AS D
  ON date_spine.dt = D.ds
    AND A.user_id = D.creator_user_id
LEFT JOIN user_invite AS E
  ON date_spine.dt = E.ds
    AND A.user_id = E.creator_user_id
LEFT JOIN user_egaf_events AS F
  ON date_spine.dt = F.ds
    AND A.user_id = F.entity_id
-- using only latest information for pendo responses
LEFT JOIN {{ ref('wrk_pendo_poll_response_pivot') }} AS H
  ON A.user_id = H.user_id
LEFT JOIN user_chart AS K
  ON date_spine.dt = K.ds
    AND A.user_id = K.creator_user_id
LEFT JOIN {{ ref('src_manager_user_onboarding') }} AS L
  ON date_spine.dt = L.ds
    AND A.user_id = L.user_id
LEFT JOIN user_database AS M
  ON date_spine.dt = M.ds
    AND A.user_id = M.creator_user_id
LEFT JOIN user_saved_query AS N
  ON date_spine.dt = N.ds
    AND A.user_id = N.creator_user_id
