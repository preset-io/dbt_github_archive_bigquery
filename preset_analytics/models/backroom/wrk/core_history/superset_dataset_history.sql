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
    CONCAT(A.workspace_id, '_', A.id) AS dataset_key,
    A.id AS dataset_id,
    A.uuid,
    A.name AS dataset_name,
    A.database_id,
    A.schema,
    D.is_example,
    -- A.column_count,
    A.metric_count,
    A.dataset_type,
    A.main_dttm_col,
    A.offset,
    A.cache_timeout,
    A.filter_select_enabled,
    A.is_featured,
    A.is_sqllab_view,
    A.is_managed_externally,
    A.is_certified,

    {{ workspace_attributes(alias="C") }}
    {{ database_engine_fields('database_engine') }}
    {{ team_attributes(alias="B") }}

    -- common fields
    A.ds,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
    A.superset_creator_user_id,
    A.superset_last_modified_user_id,
FROM date_spine
LEFT JOIN {{ ref('src_superset_dataset')}} AS A
    ON date_spine.dt >= A.effective_from
        AND date_spine.dt < COALESCE(A.effective_to, '3000-01-01')
LEFT JOIN {{ ref('manager_team_history') }} AS B
    ON A.team_id = B.team_id
        AND date_spine.dt = B.ds
LEFT JOIN {{ ref('wrk_manager_workspace') }} AS C
    ON A.workspace_id = C.workspace_id
        AND date_spine.dt = C.ds
LEFT JOIN {{ ref('src_superset_database_connection') }} AS D
    ON A.workspace_id = D.workspace_id
        AND A.database_id = D.id
        AND date_spine.dt >= D.effective_from
        AND date_spine.dt < COALESCE(D.effective_to, '3000-01-01')
