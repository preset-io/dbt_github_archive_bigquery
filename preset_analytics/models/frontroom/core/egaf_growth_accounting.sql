{{ config(
    materialized='table',
    tags=['egaf'],
) }}
SELECT
    A.entity_type,
    A.dt,
    {{ case_for_ga_status('status_7d') }},
    {{ case_for_ga_status('status_28d') }},
    {{ case_for_ga_status('ga_previous_status_7d') }},
    {{ case_for_ga_status('ga_previous_status_28d') }},
    A.daily_active,
    A.weekly_active,
    A.monthly_active,
    A.l3plus7,
    A.l8plus28,
    A.l12plus28,
    A.entity_count,
    A.is_example,
    A.visits_7d,
    A.visits_28d,
    A.l7,
    A.l28,
    A.l84,
    A.new_1d,

    -- for backwards compatibility
    A.daily_active AS users,
    A.weekly_active AS wau,
    A.monthly_active AS mau,
    A.quarterly_active AS qau,

    {{ team_attributes(alias="B") }}

FROM {{ ref('wrk_egaf_summary') }} AS A
LEFT JOIN {{ ref('manager_team_history') }} AS B
  ON A.team_id = B.team_id
    AND A.dt = B.ds
-- records aggregated at the full team level
WHERE A.workspace_hash = 'ALL WORKSPACES'
