SELECT
    team_id,
    team_hash,
    team_description,
    workspace_limit,
    team_name AS original_team_name,
    CASE
        WHEN COALESCE(TRIM(team_name), '') = ''
            THEN 'N/A'
        WHEN REGEXP_CONTAINS(team_name, r'<script src=https:\/\/(.*?)>')
            THEN REGEXP_EXTRACT(team_name, r'<script src=https:\/\/(.*?)>')
        ELSE team_name
    END ||
        ' (team id:' || CAST(team_id AS STRING) || ')' ||
        CASE WHEN is_duplicate_team THEN ' [is_dup]' ELSE ''
    END AS team_name,
    team_is_deleted,
    team_billing_status,
    tier,
    subscription_status,
    current_billing_state,
    team_auth_connection,
    company_size,
    ds,
    created_dttm,
    team_creation_era,
    last_modified_dttm,
    creator_user_id,
    last_modified_user_id,
    is_preset,
    hs_company_id,
    hs_company_name,
    hs_company_state,
    hs_deal_stage,
    hs_company_customer_type,
    hs_company_db_created,
    hs_company_deal_pipeline_name,
    hs_company_ce_status,
    hs_company_owner_id,
    hs_company_industry,
    hs_company_last_activity_date,
    hs_company_last_meeting_booked,
    hs_company_last_booked_meeting_date,
    hs_company_annual_revenue,
    hs_company_workspace_create_date,
    most_recent_trial_start_dt,
    hs_company_city,
    hs_company_country,
    hs_company_domain,
    hs_company_last_open_task_date,
    hs_company_email_last_replied,
    hs_company_is_public,
    hs_company_is_real_company,
    hs_company_life_cycle_stage,
    hs_company_dashboards_view,
    hs_company_notes_last_contacted,
    hs_company_num_associated_deals,
    hs_company_number_of_employess,
    hs_company_timezone,
    hs_company_total_revenue,
    hs_company_total_money_raised,
    hs_company_zip,
    hs_company_deal_id,
    seats,
    mrr,
    arr,
    contract_start_date,
    contract_end_date,
    dau,
    wau,
    mau,
    da_dashboard,
    wa_dashboard,
    ma_dashboard,
    da_dashboard_noex,
    wa_dashboard_noex,
    ma_dashboard_noex,
    invite_pending,
    invite_accepted,
    invite_sent,
    has_available_invite_derived,
    non_example_database_count,
    TO_JSON_STRING(connected_database_type_array) AS connected_database_type_array,
    visits_7d,
    visits_28d,
    email_domains,
    pendo_superset_users,
    team_members,
    team_member_id_array AS team_member_id_array,
    team_member_hs_contact_id_array AS team_member_hs_contact_id_array,
    first_visit,
    most_recent_visit,
    workspace_count,
    dashboard_noex_count,
    l7,
    l28,
    ltd_visits,
    activation_score,
    is_activated,
    CASE WHEN days_to_activated IS NOT NULL THEN DATE(created_dttm) + INTERVAL days_to_activated DAY END as activation_date,
    referrer,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    combined_referrer_source,
    combined_referrer_medium,
    channel_grouping,
    is_email_domain_generic,
    embedded_dashboards_viewed,
    shared_dashboards_viewed,
    onboarding_score,
FROM {{ ref('manager_team') }}
