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
    CONCAT(A.workspace_id, '_', A.id) AS report_schedule_key,
    A.id AS report_schedule_id,
    A.name AS report_schedule_name,
    A.description AS report_schedule_description,
    A.type AS report_schedule_type,
    D.is_example,
    CONCAT(A.workspace_id, '_', A.database_id) AS database_key,
    CONCAT(A.workspace_id, '_', A.dashboard_id) AS dashboard_key,
    CONCAT(A.workspace_id, '_', A.chart_id) AS chart_key,
    A.workspace_id,
    A.is_active,

    {{ team_attributes(alias="B") }}
    {{ database_engine_fields('database_engine') }}

    -- common fields
    A.ds,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
FROM date_spine
LEFT JOIN {{ ref('src_superset_report_schedule') }} AS A
    ON date_spine.dt >= A.effective_from
        AND date_spine.dt < COALESCE(A.effective_to, '3000-01-01')
LEFT JOIN {{ ref('manager_team_history') }} AS B
    ON A.team_id = B.team_id
        AND date_spine.dt = B.ds
LEFT JOIN {{ ref('src_manager_workspace') }} AS C
    ON A.workspace_id = C.id
        AND date_spine.dt = C.ds
LEFT JOIN {{ ref('src_superset_database_connection') }} AS D
    ON A.workspace_id = D.workspace_id
        AND A.database_id = D.id
        AND date_spine.dt >= D.effective_from
        AND date_spine.dt < COALESCE(D.effective_to, '3000-01-01')
