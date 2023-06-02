{#
NOTE: "full_refresh = ..." has to do with the fact that this
model is very large and would be expensive to run often
#}

{{ warn_on_full_refresh_attempt(this) }}

{{ config(
    materialized='incremental',
    full_refresh = var("super_full_refresh_use_with_care"),
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}

SELECT
    A.dt,
    A.dttm,
    A.action,
    A.event_name,
    A.event_type,
    A.duration,
    A.environment,
    A.event_id,
    A.referrer,
    A.superset_user_id,
    A.url_path,
    COALESCE(A.manager_user_id, F.manager_user_id) AS manager_user_id,
    G.hs_contact_id,
    A.source,
    A.source_id,
    A.ajs_user_id,
    A.anonymous_id,
    A.useragent,

    -- Chart
    A.chart_key,
    B.chart_name,
    B.viz_type,
    B.dataset_key,
    B.database_key,
    B.dataset_name,

    -- Dashboard
    A.dashboard_key,
    C.dashboard_title,
    C.dashboard_description,
    C.slug AS dashboard_slug,

    {{ workspace_attributes(alias="E") }}
    {{ database_engine_fields('database_engine') }}

    -- Getting the team_id from the left table since we have early-arriving facts
    A.team_id,
    {{ team_attributes(alias="D", include_team_id=False, include_core=False, include_mau_rank=False) }}

    D.arr,
    D.mrr,
    D.seats,

    CASE
        -- Dashboard takes precedence
        WHEN A.action = 'dashboard' AND C.dashboard_key IS NOT NULL THEN C.is_example
        WHEN B.chart_key IS NOT NULL THEN B.is_example
        WHEN C.dashboard_key IS NOT NULL THEN C.is_example
        ELSE False
    END AS is_example,

FROM {{ ref('wrk_superset_events') }} AS A
LEFT OUTER JOIN {{ ref('wrk_manager_workspace_latest') }} AS E
    ON A.workspace_id = E.workspace_id
LEFT OUTER JOIN {{ ref('superset_slice_latest') }} AS B
    ON A.chart_key = B.chart_key
LEFT OUTER JOIN {{ ref('superset_dashboard_latest') }} AS C
    ON A.dashboard_key = C.dashboard_key
LEFT OUTER JOIN {{ ref('wrk_manager_team_latest') }} AS D
    ON A.team_id = D.team_id
-- Fixing the fact that manager_user_id wasn't logged originally
LEFT OUTER JOIN {{ ref('wrk_manager_user_lookup') }} AS F
    ON A.workspace_id = F.workspace_id
        AND A.superset_user_id = F.superset_user_id
LEFT OUTER JOIN {{ ref('wrk_manager_user') }} AS G
    ON COALESCE(A.manager_user_id, F.manager_user_id) = G.user_id AND
    A.dt = G.ds
{% if is_incremental() %}
WHERE {{ generate_incremental_statement(this, date_col='A.dt') }}
{% endif %}
