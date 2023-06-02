{{ config(
  materialized='incremental',
  incremental_strategy = 'insert_overwrite',
  partition_by = {'field': 'ds', 'data_type': 'date'}
) }}

WITH latest_trial_start_dates AS (
  SELECT
    id,
    effective_from,
    effective_to,
  FROM {{ ref('manager_team_billing_status_history') }}
  WHERE billing_status = 'TRIAL'
),

team_agg AS (
  -- Getting a list of email domains for the team!
  SELECT
    A.ds,
    A.team_id,
    ARRAY_TO_STRING(ARRAY_AGG(DISTINCT B.email_domain ORDER BY B.email_domain), ',') AS email_domains,
    COALESCE(SUM(CASE WHEN B.has_superset_experience THEN 1 ELSE 0 END), 0) AS pendo_superset_users,
    COALESCE(SUM(CASE WHEN A.team_role_name = 'Admin' THEN 1 ELSE 0 END), 0) AS team_members_admin,
    COALESCE(SUM(CASE WHEN A.team_role_name = 'User' THEN 1 ELSE 0 END), 0) AS team_members_user,
    COALESCE(COUNT(DISTINCT A.user_id), 0) AS team_members,
  FROM {{ ref('src_manager_team_membership') }} AS A
  INNER JOIN {{ ref('wrk_manager_user') }} AS B
    ON A.ds = B.ds
      AND A.user_id = B.user_id
  GROUP BY 1, 2
),

recurly_agg AS (
  SELECT
    dt AS ds,
    team_hash,
    SUM(mrr_paying) AS recurly_mrr,
    SUM(arr_paying) AS recurly_arr,
    MAX(plan_code) AS recurly_plan_code,
    MAX(trial_started_at) AS recurly_trial_started_at,
    MAX(trial_ends_at) AS recurly_trial_ends_at,
    SUM(sold_seats) AS recurly_seats,
  FROM {{ ref('account_subscription_history') }}
  WHERE upgrade_status = 'standard'
  GROUP BY 1, 2 -- account_code should be unique, but preventing exploding joins
),

primary_email_domains_with_multiple_teams AS (
  SELECT
    ds,
    primary_email_domain,
  FROM {{ ref('wrk_manager_team_domain') }}
  WHERE NOT {{ is_generic_email_condition() }}
  GROUP BY 1, 2
  HAVING COUNT(primary_email_domain) > 1
),

ranked_team_agg AS (
  SELECT
    A.ds,
    A.team_id,
    ROW_NUMBER() OVER (PARTITION BY A.ds, B.primary_email_domain ORDER BY A.team_members DESC) AS email_domain_rank
  FROM team_agg AS A
  INNER JOIN {{ ref('wrk_manager_team_domain') }} AS B
    ON A.ds = B.ds
    AND A.team_id = B.team_id
  INNER JOIN primary_email_domains_with_multiple_teams AS C
    ON A.ds = C.ds
    AND B.primary_email_domain = C.primary_email_domain
),

churn_date_subq AS (
  SELECT
    team_id,
    max_dt AS churn_date,
  FROM (
    SELECT
      team_id,
      MAX(dt) AS max_dt,
    FROM {{ ref('wrk_deal_info') }} AS A
    GROUP BY 1
  )
  WHERE max_dt NOT IN (
    SELECT MAX(dt) FROM {{ ref('wrk_deal_info') }}
  )
),

first_member_info AS (
  -- Finding the first member to join the team
  -- this can be helpful, especially as a proxy for when we don't
  -- have team creator for whatever reason
  WITH initial_team_creation AS (
    SELECT
      team_id,
      MIN(created_dttm) AS created_dttm,
    FROM {{ ref('manager_team_membership_latest') }}
    GROUP BY 1
  ),

  first_user AS (
    SELECT
      A.team_id,
      MIN(A.user_id) AS first_user_id,
    FROM {{ ref('manager_team_membership_latest') }} AS A
    INNER JOIN initial_team_creation AS B
    ON A.team_id = B.team_id
      AND A.created_dttm = B.created_dttm
    GROUP BY 1
  )

  SELECT
    A.team_id,
    A.first_user_id,
    B.email_domain AS first_user_email_domain,
  FROM first_user AS A
  INNER JOIN {{ ref('wrk_manager_user_latest') }} AS B
    ON A.first_user_id = B.user_id
)

SELECT
  A.ds,
  A.id AS team_id,
  A.team_hash,
  A.description AS team_description,
  A.workspace_limit,
  A.title AS team_name,
  A.deleted AS team_is_deleted,
  A.is_hibernated,
  A.billing_status AS team_billing_status,
  A.tier,
  A.initial_signup_tier,
  A.subscription_status,
  A.new_billing_status as current_billing_state,
  A.billing_frequency,
  A.billing_method,
  A.auth_connection AS team_auth_connection,
  A.created_dttm,
  CASE
    WHEN A.created_dttm < {{ var('pre_beta_start_date') }} THEN 'pre-beta'
    WHEN A.created_dttm < {{ var('beta_start_date') }} THEN 'beta'
    ELSE 'GA'
  END AS team_creation_era,
  A.last_modified_dttm,
  A.creator_user_id,
  A.last_modified_user_id,
  COALESCE(NOT (M.team_id IS NULL AND A.title NOT IN ('CypressPol')), False) AS is_preset,
  D.first_user_id,
  D.first_user_email_domain,
  G.email_domains,
  G.pendo_superset_users,
  G.team_members,
  G.team_members_admin,
  G.team_members_user,
  F.primary_email_domain,
  COALESCE(H.email_domain_rank > 1, False) AS is_duplicate_team,

  -- from Hubspot
  COALESCE(
    DATE(B2.hs_company_trial_begin), DATE(E.effective_from)
  ) AS most_recent_trial_start_dt,
  B2.hs_company_id,
  B2.hs_company_name,
  B2.hs_company_state,
  B2.hs_deal_stage,
  B2.hs_company_customer_type,
  B2.hs_company_db_created,
  B2.hs_company_deal_pipeline_name,
  B2.hs_company_ce_status,
  B2.hs_company_owner_id,
  B2.hs_company_industry,
  B2.hs_company_last_activity_date,
  B2.hs_company_last_meeting_booked,
  B2.hs_company_last_booked_meeting_date,
  B2.hs_company_annual_revenue,
  B2.hs_company_workspace_create_date,
  B2.hs_company_city,
  B2.hs_company_country,
  B2.hs_company_domain,
  B2.hs_company_last_open_task_date,
  B2.hs_company_email_last_replied,
  B2.hs_company_is_public,
  B2.hs_company_is_real_company,
  B2.hs_company_life_cycle_stage,
  B2.hs_company_dashboards_view,
  B2.hs_company_notes_last_contacted,
  B2.hs_company_num_associated_deals,
  B2.hs_company_number_of_employess,
  B2.hs_company_timezone,
  B2.hs_company_total_revenue,
  B2.hs_company_total_money_raised,
  B2.hs_company_zip,
  B2.hs_company_deal_id,
  B2.contract_end_date,
  COALESCE(DATE(B2.contract_start_date)) AS contract_start_date,
  CASE
    WHEN billing_status = 'ENTERPRISE' AND K.arr > 0
      THEN 'ENTERPRISE'
    WHEN billing_status = 'ENTERPRISE'
    THEN 'ENTERPRISE_TRIAL'
    ELSE billing_status
  END AS team_billing_status_derived,
  I.recurly_mrr,
  I.recurly_arr,
  I.recurly_seats,
  I.recurly_plan_code,
  I.recurly_trial_started_at,
  I.recurly_trial_ends_at,
  K.has_open_renewal_deal,
  K.seats AS sales_led_seats,
  K.number_of_creator_licenses,
  K.number_of_viewer_licenses,
  K.number_of_embedded_view_licenses,
  K.mrr AS sales_led_mrr,
  K.arr AS sales_led_arr,
  COALESCE(I.recurly_mrr, 0) + COALESCE(K.mrr, 0) AS mrr,
  COALESCE(I.recurly_arr, 0) + COALESCE(K.arr, 0) AS arr,
  COALESCE(I.recurly_seats, 0) + COALESCE(K.seats, 0) AS seats,
  COALESCE(C.is_primary_team_for_company, FALSE) AS is_primary_team_for_company,
  L.referrer,
  L.utm_source,
  L.utm_medium,
  L.utm_campaign,
  L.utm_content,
  L.utm_term,
  L.combined_referrer_source,
  L.combined_referrer_medium,
  L.channel_grouping,
  CASE
    WHEN {{ is_generic_email_condition() }} THEN TRUE
    WHEN F.primary_email_domain IS NOT NULL THEN FALSE
  END AS is_email_domain_generic,
  CASE
    WHEN primary_email_domain IS NULL THEN 'null'
    WHEN {{ is_generic_email_condition() }} THEN 'generic'
    WHEN LOWER(F.primary_email_domain) LIKE '%.edu' OR
         LOWER(F.primary_email_domain) LIKE '%.edu.%' THEN 'edu'
    ELSE 'company'
  END AS email_domain_category,
  N.churn_date,
  --  N.email IS NOT NULL AS is_superset_slack_member,
FROM {{ ref('src_manager_team') }} AS A
LEFT JOIN {{ ref('wrk_company_team_map') }} AS C
  ON A.id = C.team_id
    AND C.is_primary_company_for_team
LEFT JOIN {{ ref('hs_companies') }} AS B2
  ON C.company_id = B2.hs_company_id
LEFT JOIN first_member_info AS D
  ON A.id = D.team_id
LEFT JOIN latest_trial_start_dates AS E
  ON A.id = E.id
    AND A.ds >= COALESCE(E.effective_from, '2000-01-01')
    AND A.ds < COALESCE(E.effective_to, '2100-01-01')
LEFT JOIN {{ ref('wrk_manager_team_domain') }} AS F
  ON A.ds = F.ds
    AND A.id = F.team_id
LEFT JOIN team_agg AS G
  ON A.ds = G.ds
    AND A.id = G.team_id
LEFT JOIN ranked_team_agg AS H
  ON A.ds = H.ds
    AND A.id = H.team_id
LEFT JOIN recurly_agg AS I
  ON A.team_hash = I.team_hash
    AND A.ds = I.ds
LEFT JOIN {{ ref('wrk_deal_info') }} AS K
  ON A.ds = K.dt
    AND A.id = K.team_id
LEFT JOIN {{ ref( 'wrk_user_utm_attribution' ) }} AS L
  ON SAFE_CAST(A.creator_user_id AS STRING) = L.blended_user_id
LEFT JOIN {{ ref('preset_teams') }} AS M
    ON A.ds = M.ds
        AND A.id = M.team_id
LEFT JOIN churn_date_subq AS N ON N.team_id = A.id
-- LEFT JOIN airbyte.s3_csv_slack_members AS N
--  ON A.email = N.email
WHERE A.ds >= {{ var("start_date") }}
  AND A.ds < CURRENT_DATE()
  {% if is_incremental() %}
  AND {{ generate_incremental_statement(this, date_col='A.ds', this_date_col='ds') }}
  {% endif %}
