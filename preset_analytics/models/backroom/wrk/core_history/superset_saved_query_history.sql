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
    CONCAT(A.workspace_id, '_', A.id) AS saved_query_key,
    A.id AS saved_query_id,
    A.label AS saved_query_label,
    A.description,
    A.last_run,
    A.char_length,
    False AS is_example,

    {{ workspace_attributes(alias="C") }}
    {{ database_engine_fields('database_engine') }}
    {{ team_attributes(alias="B") }}

    -- common fields
    A.ds,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
FROM date_spine
LEFT JOIN {{ ref('src_superset_saved_query') }} AS A
    ON date_spine.dt >= A.effective_from
        AND date_spine.dt < COALESCE(A.effective_to, '3000-01-01')
LEFT JOIN {{ ref('manager_team_history') }} AS B
    ON A.team_id = B.team_id
        AND date_spine.dt = B.ds
LEFT JOIN {{ ref('wrk_manager_workspace') }} AS C
    ON A.workspace_id = C.workspace_id
        AND date_spine.dt = C.ds
