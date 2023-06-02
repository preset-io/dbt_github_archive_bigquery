{{ config(
    alias='wrk_egaf_team_retention_status_history',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'dt','data_type': 'date'},
    tags=['egaf', 'growth']
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
  A.dt,
  A.entity_id as team_id,
  A.workspace_hash,
  A.entity_type,
  A.l7,
  A.yesterday_l7,
  A.previous_l7,
  A.l28,
  A.yesterday_l28,
  A.previous_l28,
  {{ growth_accounting_case_expr('A.dt', 7) }} AS status_7d,
  {{ growth_accounting_case_expr('A.dt', 28) }} AS status_28d,
  {{ growth_accounting_case_expr('A.dt', 7, framing='previous') }} AS ga_previous_status_7d,
  {{ growth_accounting_case_expr('A.dt', 28, framing='previous') }} AS ga_previous_status_28d
FROM
  date_spine AS ds
LEFT JOIN
  {{ ref('wrk_egaf_event_agg') }} AS A
ON ds.dt = A.dt
LEFT JOIN
  {{ ref('wrk_egaf_first_event') }} AS B
ON A.entity_id = B.entity_id
  AND A.team_id = COALESCE(B.team_id, -1)
  AND A.entity_type = B.entity_type
WHERE
  A.entity_type = 'team'
