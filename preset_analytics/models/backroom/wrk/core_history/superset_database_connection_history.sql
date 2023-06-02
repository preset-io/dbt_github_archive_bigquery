{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'}
) }}



WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
    {% if is_incremental() %}
    WHERE {{ generate_incremental_statement(this) }}
    {% endif %}
)

SELECT
    date_spine.dt,
    CONCAT(A.workspace_id, '_', A.id) AS database_connection_key,
    A.id AS database_connection_id,
    A.name AS database_connection_name,
    A.is_example,

    C.workspace_id,
    C.workspace_hash,
    C.workspace_title,
    C.workspace_hostname,
    C.workspace_region,
    CASE
        WHEN INSTR(A.engine, '+') > 0 THEN SUBSTR(A.engine, 0, INSTR(A.engine, '+') - 1)
        ELSE A.engine
    END AS database_engine,
    A.engine AS database_driver,
    COALESCE(A.team_id, B.team_id) AS team_id,
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

    -- common fields
    A.ds,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
FROM date_spine
LEFT JOIN {{ ref('src_superset_database_connection') }} AS A
    ON date_spine.dt >= A.effective_from
        AND date_spine.dt < COALESCE(A.effective_to, '3000-01-01')
LEFT JOIN {{ ref('wrk_manager_team') }} AS B
    ON date_spine.dt = B.ds
        AND A.team_id = B.team_id
LEFT JOIN {{ ref('wrk_manager_workspace') }} AS C
    ON date_spine.dt = C.ds
        AND A.workspace_id = C.workspace_id
