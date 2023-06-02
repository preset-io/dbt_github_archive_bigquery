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
    A.ds,
    A.team_id,
    A.workspace_id,
    A.dashboard_key,
    A.chart_key,
    A.dashboard_id,
    A.chart_id,
FROM date_spine
LEFT JOIN {{ ref('src_superset_dashboard_chart') }} AS A
    ON date_spine.dt >= A.effective_from
        AND date_spine.dt < COALESCE(A.effective_to, '3000-01-01')
