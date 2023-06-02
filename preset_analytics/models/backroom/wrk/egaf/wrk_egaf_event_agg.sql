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
    ds.dt,
    egaf.entity_type,
    egaf.team_id as team_id,
    egaf.workspace_hash,
    egaf.is_example,
    egaf.entity_id,
    -- Is the USER DAU?
    MAX(CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) = 0 THEN 1 END) AS daily_active,
    -- Is the USER WAU?
    COUNT(DISTINCT CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 0 AND 6 THEN egaf.dt END) AS l7,
    -- Was the USER WAU yesterday?
    COUNT(DISTINCT CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 1 AND 7 THEN egaf.dt END) AS yesterday_l7,
    -- Was the USER WAU the previous week
    COUNT(DISTINCT CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 7 AND 13 THEN egaf.dt END) AS previous_l7,
    -- Is the USER MAU?
    COUNT(DISTINCT CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 0 AND 27 THEN egaf.dt END) AS l28,
    -- Was the USER MAU yesterday?
    COUNT(DISTINCT CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 1 AND 28 THEN egaf.dt END) AS yesterday_l28,
    -- Was the USER WAU the previous month
    COUNT(DISTINCT CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 28 AND 55 THEN egaf.dt END) AS previous_l28,
    -- Is the USER MAU?
    COUNT(DISTINCT CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 0 AND 83 THEN egaf.dt END) AS l84,
    SUM(CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) = 0 THEN 1 ELSE 0 END) AS visits_1d,
    SUM(CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 0 AND 6 THEN 1 ELSE 0 END) AS visits_7d,
    SUM(CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 0 AND 27 THEN 1 ELSE 0 END) AS visits_28d,
    SUM(CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) = 0 THEN egaf.events ELSE 0 END) AS events_1d,
    SUM(CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 0 AND 6 THEN egaf.events ELSE 0 END) AS events_7d,
    SUM(CASE WHEN DATE_DIFF(ds.dt, egaf.dt, DAY) BETWEEN 0 AND 27 THEN egaf.events ELSE 0 END) AS events_28d,
FROM date_spine AS ds
LEFT JOIN {{ ref('wrk_egaf_events') }} AS egaf
    ON egaf.dt <= ds.dt
        AND egaf.dt >= DATE_ADD(ds.dt, INTERVAL -90 DAY)
GROUP BY 1, 2, 3, 4, 5, 6
