{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'ds', 'data_type': 'date'}
) }}



WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this, this_date_col='ds') }}
    {% endif %}
)

SELECT
    date_spine.dt AS ds,
    A.workspace_id,
    A.workspace_hash,
    A.workspace_title,
    A.workspace_hostname,
    A.workspace_region,
    A.workspace_description,
    A.allow_public_dashboards,
    A.last_accessed_at,
    B.team_id,
    B.team_name,
    B.team_is_deleted,
    B.team_billing_status,
    B.team_billing_status_derived,
    B.is_preset,
    B.hs_company_id,
    B.hs_company_name,
    B.hs_company_state,
    B.hs_deal_stage,
    B.hs_company_customer_type,
    B.hs_company_db_created,
    B.hs_company_deal_pipeline_name,
    B.hs_company_ce_status,
    B.hs_company_owner_id,
    B.hs_company_last_activity_date,
    B.team_creation_era,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
    A.workspace_status,
    DATE_DIFF(current_date(), GREATEST(DATE(A.last_accessed_at), DATE(A.last_modified_dttm)), DAY) AS days_since_last_workspace_activity,
FROM date_spine
LEFT JOIN {{ ref('wrk_manager_workspace') }} AS A
    ON date_spine.dt = A.ds
LEFT JOIN {{ ref('wrk_manager_team') }} AS B
    ON A.team_id = B.team_id
        AND date_spine.dt = B.ds
