{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
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
    A.chart_key,
    A.chart_id,
    A.chart_name,
    A.viz_type,
    A.is_example,
    A.dataset_key,
    A.database_key,
    D.is_example AS db_is_example,
    A.datasource_name AS dataset_name,
    F.dataset_type,

    {{ workspace_attributes(alias="C") }}
    A.team_id,
    {{ team_attributes(alias="B", include_team_id=False) }}
    {{ database_engine_fields('engine', alias='D') }}

    -- common fields
    A.ds,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
    E.ltd_views,
    E.ltd_users,
    {%- for i in var("day_counts") %}
        {{- 'E.n' ~ i ~ 'd_views' | indent(4) }},
        {{- 'E.n' ~ i ~ 'd_users' | indent(4) }},
    {%- endfor %}
    G.num_dashboards_used_in,
    G.dashboard_keys_used_in,
FROM date_spine
LEFT JOIN {{ ref('src_superset_slice') }} AS A
    ON date_spine.dt >= A.effective_from
        AND date_spine.dt < COALESCE(A.effective_to, '3000-01-01')
LEFT JOIN {{ ref('manager_team_history') }} AS B
    ON date_spine.dt = B.ds
        AND A.team_id = B.team_id
LEFT JOIN {{ ref('wrk_manager_workspace') }} AS C
    ON date_spine.dt = C.ds
        AND A.workspace_id = C.workspace_id
LEFT JOIN {{ ref('src_superset_database_connection') }} AS D
    ON date_spine.dt = D.ds
        AND A.workspace_id = D.workspace_id
        AND A.database_id = D.id
LEFT JOIN {{ ref('wrk_action_actor_accounting_summary') }} AS E
    ON date_spine.dt = E.ds
        AND A.team_id = E.team_id
        AND A.chart_key = E.object_key
        AND E.object_type = 'chart'
LEFT JOIN {{ ref('superset_dataset_history') }} AS F
    ON date_spine.dt = F.dt
        AND A.dataset_key = F.dataset_key
LEFT JOIN {{ ref('wrk_superset_chart_dashboard')}} AS G
    ON date_spine.dt = G.dt
        AND A.chart_key = G.chart_key
