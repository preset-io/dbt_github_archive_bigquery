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
    A.dashboard_key,
    A.dashboard_id,
    A.uuid,
    A.dashboard_title,
    A.dashboard_description,
    A.is_untitled,
    A.slug,
    A.description,
    A.chart_count,
    A.json_metadata,
    A.selected_color_scheme,
    A.is_example,
    A.has_filter,
    A.has_divider,
    A.has_both_filter_divider,
    A.has_only_filter,
    A.has_only_divider,
    A.published,
    A.certified_by,
    A.certification_details,
    A.is_managed_externally,

    {{ workspace_attributes(alias="C") }}

    A.team_id,
    {{ team_attributes(alias="B", include_team_id=False) }}

    -- common fields
    A.ds,
    A.created_dttm,
    A.last_modified_dttm,
    A.creator_user_id,
    A.last_modified_user_id,
    A.superset_creator_user_id,
    A.superset_last_modified_user_id,

    D.ltd_views,
    D.ltd_users,
    {%- for i in var("day_counts") %}
        {{- 'D.n' ~ i ~ 'd_views' | indent(4) }},
        {{- 'D.n' ~ i ~ 'd_users' | indent(4) }},
    {%- endfor %}
    E.chart_keys_in_dashboard,
FROM date_spine
LEFT JOIN {{ ref('src_superset_dashboard') }} AS A
    ON date_spine.dt >= A.effective_from
        AND date_spine.dt < COALESCE(A.effective_to, '3000-01-01')
LEFT JOIN {{ ref('manager_team_history') }} AS B
    ON date_spine.dt = B.ds
        AND A.team_id = B.team_id
LEFT JOIN {{ ref('wrk_manager_workspace') }} AS C
    ON date_spine.dt = C.ds
        AND A.workspace_id = C.workspace_id
        AND A.team_id = C.team_id
LEFT JOIN {{ ref('wrk_action_actor_accounting_summary') }} AS D
    ON date_spine.dt = D.ds
        AND A.team_id = D.team_id
        AND CONCAT(A.workspace_id, '_', A.id) = D.object_key
        AND D.object_type = 'dashboard'
LEFT JOIN {{ ref('wrk_superset_dashboard_chart') }} AS E
    ON date_spine.dt = E.dt
        AND A.team_id = E.team_id
        AND A.workspace_id = E.workspace_id
        AND A.dashboard_key = E.dashboard_key
