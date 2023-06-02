{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'dt','data_type': 'date'},
    tags=['egaf']
) }}

{% set process_to_dt = latest_dt(ref('superset_event_log'), "dt", previous_day=False) %}

WITH date_spine AS (
    SELECT dt,
    FROM {{ ref('date_spine') }}
    WHERE dt <= DATE("{{ process_to_dt }}") -- we only want full days to be reflected
        {% if is_incremental() %}
        AND {{ generate_incremental_statement(this) }}
    {% endif %}
)

SELECT
    A.team_id,
    A.workspace_hash,
    A.entity_type,
    A.is_example,
    A.dt,
    {{ growth_accounting_case_expr('A.dt', 7) }} AS status_7d,
    {{ growth_accounting_case_expr('A.dt', 28) }} AS status_28d,
    {{ growth_accounting_case_expr('A.dt', 7, framing='previous') }} AS ga_previous_status_7d,
    {{ growth_accounting_case_expr('A.dt', 28, framing='previous') }} AS ga_previous_status_28d,
    ---------------------------------------------------------------
    -- These 2 fields enable for "L7 distribution" over time, but may
    -- explode the summary data quite a bit
    A.l7,
    A.l28,
    A.l84,
    ---------------------------------------------------------------

    COUNT(A.entity_id) AS entity_count,
    SUM(A.daily_active) AS daily_active,
    SUM(CASE WHEN A.l7 >= 1 THEN 1 ELSE 0 END) AS weekly_active,
    SUM(CASE WHEN A.l28 >= 1 THEN 1 ELSE 0 END) AS monthly_active,
    SUM(CASE WHEN A.l84 >= 1 THEN 1 ELSE 0 END) AS quarterly_active,
    SUM(CASE WHEN A.l7 >= 3 THEN 1 ELSE 0 END) AS l3plus7,
    SUM(CASE WHEN A.l28 >= 8 THEN 1 ELSE 0 END) AS l8plus28,
    SUM(CASE WHEN A.l28 >= 12 THEN 1 ELSE 0 END) AS l12plus28,
    SUM(A.visits_1d) AS visits_1d,
    SUM(A.visits_7d) AS visits_7d,
    SUM(A.visits_28d) AS visits_28d,
    SUM(A.events_1d) AS events_1d,
    SUM(A.events_7d) AS events_7d,
    SUM(A.events_28d) AS events_28d,
    -- Is the user new today?
    SUM(CASE WHEN DATE_DIFF(A.dt, B.first_event, DAY) = 0 THEN 1 ELSE 0 END) AS new_1d,
FROM date_spine AS ds
LEFT JOIN {{ ref('wrk_egaf_event_agg') }} AS A
    ON ds.dt = A.dt
LEFT JOIN {{ ref('wrk_egaf_first_event') }} AS B
    ON A.entity_id = B.entity_id
        AND A.team_id = COALESCE(B.team_id, -1)
        AND A.entity_type = B.entity_type
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
