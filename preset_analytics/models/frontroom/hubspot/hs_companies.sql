{{
  config(materialized = "view")
}}
--- Group by id as a company can have more than 1 deal
--- Deal is used to identify the companies that we are working with

WITH company_deal_agg AS (
    SELECT
        c.company_id AS hs_company_id,
        MAX(di.deal_id) AS hs_company_deal_id,
        MAX(di.dealname) AS hs_company_deal_pipeline_name,
        MAX(di.dealtype) AS hs_deal_stage,
        SUM(CASE WHEN di.hs_is_closed_won THEN di.amount END) AS deal_amount,
        MAX(di.number_of_seats) AS seats,
        --MAX(TIMESTAMP_MILLIS(CAST(di.contract_start_date AS INT64))) AS contract_start_date,
        MAX(di.contract_start_date) AS contract_start_date,
        --MAX(TIMESTAMP_MILLIS(CAST(di.contract_end_date AS INT64))) AS contract_end_date,
        MAX(di.contract_end_date) AS contract_end_date,
    FROM {{ ref('stg_hubspot__deal_company') }} AS c
    LEFT JOIN {{ ref('hubspot__deals') }} AS di
        ON c.deal_id = di.deal_id
    GROUP BY 1
)

SELECT
    c.company_id as hs_company_id,
    c.name AS hs_company_name,
    c.annualrevenue AS hs_company_annual_revenue,
    c.ce_workspacecreateddate AS hs_company_workspace_create_date,
    c.ce_trialbegindate AS hs_company_trial_begin,
    c.city AS hs_company_city,
    c.country AS hs_company_country,
    c.customer_type AS hs_company_customer_type,
    c.domain AS hs_company_domain,
    c.engagements_last_meeting_booked AS hs_company_last_meeting_booked,
    c.hs_last_booked_meeting_date AS hs_company_last_booked_meeting_date,
    c.hs_last_open_task_date AS hs_company_last_open_task_date,
    c.ce_lastactivitydate AS hs_company_last_activity_date,
    c.hs_sales_email_last_replied AS hs_company_email_last_replied,
    c.industry AS hs_company_industry,
    c.is_public AS hs_company_is_public,
    c.is_real_company_ AS hs_company_is_real_company,
    c.lifecyclestage AS hs_company_life_cycle_stage,
    CAST(c.ce_dashboards AS INT64) AS hs_company_dashboards_view,
    c.ce_dbcreated AS hs_company_db_created,
    c.ce_status AS hs_company_ce_status,
    c.hubspot_owner_id AS hs_company_owner_id,
    c.notes_last_contacted AS hs_company_notes_last_contacted,
    c.num_associated_deals AS hs_company_num_associated_deals,
    c.numberofemployees AS hs_company_number_of_employess,
    c.preset_team_id AS hs_company_preset_team_id,
    c.state AS hs_company_state,
    c.timezone AS hs_company_timezone,
    c.total_revenue AS hs_company_total_revenue,
    c.total_money_raised AS hs_company_total_money_raised,
    c.zip AS hs_company_zip,
    cda.hs_company_deal_id,
    cda.hs_company_deal_pipeline_name,
    cda.hs_deal_stage,
    cda.deal_amount,
    SAFE_DIVIDE(cda.deal_amount, DATE_DIFF(DATE(cda.contract_end_date), DATE(cda.contract_start_date), MONTH)) AS mrr,
    SAFE_DIVIDE(cda.deal_amount, DATE_DIFF(DATE(cda.contract_end_date), DATE(cda.contract_start_date), MONTH)) * 12 AS arr,
    cda.seats,
    cda.contract_start_date,
    cda.contract_end_date,
FROM {{ ref('hubspot__companies') }} AS c
LEFT JOIN company_deal_agg AS cda
    ON c.company_id = cda.hs_company_id
