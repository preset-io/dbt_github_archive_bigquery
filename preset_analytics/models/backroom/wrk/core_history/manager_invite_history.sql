{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'ds', 'data_type': 'date'},
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
    A.id AS invite_id,
    A.status AS invite_status,
    A.accepted_by_user_id,
    A.accepted_at,
    A.team_role_name,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
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
FROM date_spine
LEFT JOIN {{ ref('src_manager_invite') }} AS A
    ON date_spine.dt = A.ds
LEFT JOIN {{ ref('wrk_manager_team') }} AS B
    ON date_spine.dt = B.ds
        AND A.team_id = B.team_id
-- This seems to indicate that the team was created in the "CS TOOL"
-- the invite IS NOT a user-generated invite
WHERE A.creator_user_id IS NOT NULL

    -- Filtering out when the user is the creator of the team, if we do
    -- not have a creator for the team (for historical reasons),
    -- we use the first member
    AND COALESCE(A.accepted_by_user_id, -1) != COALESCE(B.creator_user_id, B.first_user_id, -1)
