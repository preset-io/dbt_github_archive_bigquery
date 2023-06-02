{{ config(
  materialized='incremental',
  incremental_strategy = 'insert_overwrite',
  partition_by = {'field': 'ds', 'data_type': 'date'},
) }}

{% set billing_status_map = [['"FREE"',0],['"TRIAL"',1],['"TRIAL_EXPIRED"',2],['"PAID"',3],['"ENTERPRISE"',4]] -%}

WITH hubspot_company_map AS (
  SELECT
    contact_id,
    CAST(associatedcompanyid AS INT) AS hs_company_id,
  FROM {{ ref('hubspot__contacts') }}
),

hubspot_contact_list_membership AS (
  SELECT
    contact_id,
    ARRAY_AGG(contact_list_id) AS hs_list_id_array,
  FROM {{ ref('stg_hubspot__contact_list_member') }}
  GROUP BY 1
),

account_subscription AS (
  SELECT
    email,
    SUM(arr_paying) AS arr_paying,
    SUM(arr_trial) AS arr_trial,
    MAX(trial_status) AS trial_status,
    MAX(plan_code) AS plan_code,
  FROM {{ ref('account_subscription_latest') }}
  WHERE upgrade_status = 'standard'
  GROUP BY 1
),

user_team_membership AS (
  SELECT
    membership.ds,
    membership.user_id,
    ARRAY_AGG(membership.team_id) AS team_id_array,
    ARRAY_AGG(membership.team_role_name) AS team_role_array,
    MAX({{ switch("team.billing_status", billing_status_map, 'left') }}) AS highest_billing_status_rank
  FROM {{ ref('src_manager_team_membership') }} AS membership
  LEFT JOIN {{ ref('src_manager_team')}} AS team
    ON membership.ds = team.ds
      AND membership.team_id = team.id
  GROUP BY 1, 2
),

user_workspace_membership AS (
  SELECT
    A.*,
    B.role_identifier AS highest_workspace_role,
  FROM (
    SELECT
      membership.ds,
      membership.user_id,
      ARRAY_AGG(DISTINCT membership.workspace_id) AS workspace_id_array,
      ARRAY_AGG(DISTINCT membership.role_identifier) AS workspace_role_array,
      COUNT(
        CASE
          WHEN membership.role_identifier IN (
            'Workspace Admin',
            'Primary Contributor',
            'Limited Contributor'
          )
            THEN membership.workspace_id
        END) > 0 AS is_creator,
        COUNT(
        CASE
          WHEN membership.role_identifier IN (
            'Dashboard Viewer',
            'Viewer'
          )
            THEN membership.workspace_id
        END) > 0 AS is_viewer,
      MAX(role_rank) AS role_rank,
    FROM {{ ref('src_manager_workspace_membership') }} AS membership
    GROUP BY 1, 2
  ) AS A
  LEFT JOIN {{ ref('role_rank_lookup') }} AS B ON A.role_rank = B.role_rank
),

user_creators AS (
  SELECT DISTINCT
    ds,
    creator_user_id,
  FROM {{ ref('src_manager_team') }}
)

SELECT
  A.ds,
  A.user_id,
  A.is_preset,
  A.active,
  A.last_login,
  A.login_count,
  A.fail_login_count,
  A.onboarded,
  COALESCE(A.company_name, K.company_name) AS company_name,
  COALESCE(A.company_category, K.company_category) AS company_category,
  COALESCE(A.company_size, K.company_size) AS company_size,
  CASE COALESCE(A.company_size, K.company_size)
    WHEN "SELF_EMPLOYED" THEN 1
    WHEN "EMPLOYEES_2_5" THEN 2
    WHEN "EMPLOYEES_6_100" THEN 3
    WHEN "EMPLOYEES_101_1000" THEN 4
    WHEN "EMPLOYEES_1001_10000" THEN 5
    WHEN "EMPLOYEES_10001_PLUS" THEN 6
  END AS company_size_rank,
  A.description,
  A.created_dttm,
  A.last_modified_dttm,
  A.creator_user_id,
  A.last_modified_user_id,
  A.email,
  A.first_name,
  A.last_name,
  A.email_marketing,
  E.hs_list_id_array,
  CASE
    WHEN INSTR(A.email, '@') > 0
      THEN SUBSTR(A.email, INSTR(A.email, '@') + 1)
  END AS email_domain,
  CAST(C.contact_id AS INT) AS hs_contact_id,
  B.hs_company_id,
  COALESCE(H.pendo_used_superset, 'Unknown') AS pendo_used_superset,
  COALESCE(H.pendo_department, 'Unknown') AS pendo_department,
  COALESCE(H.pendo_role, 'Unknown') AS pendo_role,

  -- Recurly fields
  COALESCE(I.arr_paying, 0) AS recurly_arr_paying,
  COALESCE(I.arr_trial, 0) AS recurly_arr_trial,
  I.trial_status AS recurly_trial_status,
  I.plan_code AS recurly_plan_code,

  -- from manager_user_onboarding
  COALESCE(J.has_superset_experience, H.pendo_used_superset = 'Yes') AS has_superset_experience,
  COALESCE(J.department, H.pendo_department, 'Unknown') AS department,
  COALESCE(J.role, H.pendo_role, 'Unknown') AS role,
  J.intent_choice_evaluator,
  J.intent_choice_connector,
  J.intent_choice_builder,
  J.intent_choice_consumer,
  J.intent_choice_other,

  T.team_id_array,
  T.team_role_array,
  {{ switch("T.highest_billing_status_rank", billing_status_map, 'right') }} AS highest_billing_status,
  O.workspace_id_array,
  O.workspace_role_array,
  O.is_creator,
  O.is_viewer,
  O.highest_workspace_role,
  G.creator_user_id IS NOT NULL AS is_team_creator,

FROM {{ ref('src_manager_preset_user') }} AS A
LEFT JOIN {{ ref('wrk_map_email_to_hs_contact') }} AS C
  ON A.email = C.email
LEFT JOIN hubspot_company_map AS B
  ON C.contact_id = B.contact_id
LEFT JOIN hubspot_contact_list_membership AS E
  ON C.contact_id = E.contact_id
LEFT JOIN {{ ref('wrk_pendo_poll_response_pivot') }} AS H
  ON A.user_id = H.user_id
LEFT JOIN account_subscription AS I
  ON A.email = I.email
LEFT JOIN {{ ref('src_manager_user_onboarding') }} AS J
  ON A.ds = J.ds
    AND A.user_id = J.user_id
LEFT JOIN {{ ref('src_manager_user_details') }} AS K
  ON A.ds = K.ds
    AND A.user_id = K.user_id
LEFT JOIN user_team_membership AS T
  ON A.ds = T.ds
    AND A.user_id = T.user_id
LEFT JOIN user_workspace_membership AS O
  ON A.ds = O.ds
    AND A.user_id = O.user_id
LEFT JOIN user_creators AS G
  ON A.ds = G.ds
    AND A.user_id = G.creator_user_id
{% if is_incremental() %}
WHERE {{ generate_incremental_statement(this, date_col='A.ds', this_date_col='ds') }}
{% endif %}
