{{ config(
    alias='time_to_dashboard',
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}

WITH WDS as ( --Workspace Date Spine
  SELECT
    dt,
    workspace_id,
    team_id,
    workspace_created_dttm,
    workspace_created_date
  FROM
    {{ ref('workspace_date_spine') }}
  {% if is_incremental() %}
  WHERE {{ generate_incremental_statement(this) }}
  {% endif %}
),

SSDBP AS ( --Source Superset DashBoard Prep
  SELECT
    team_id,
    workspace_id,
    MIN(created_dttm) as first_noex_dashboard_created_dttm
  FROM
    {{ ref('src_superset_dashboard') }}
  WHERE
    NOT is_example
    AND NOT is_untitled
  GROUP BY 1, 2
),

SSDB AS (
  SELECT
    WDS.dt,
    WDS.workspace_id,
    WDS.team_id,
    WDS.workspace_created_dttm,
    FIRST_VALUE(SSDBP.first_noex_dashboard_created_dttm IGNORE NULLS) OVER (PARTITION BY WDS.workspace_id ORDER BY WDS.dt ASC) as first_noex_dashboard_created_dttm
  FROM
    WDS
  LEFT JOIN
    SSDBP
  ON WDS.dt >= SSDBP.first_noex_dashboard_created_dttm AND WDS.workspace_id = SSDBP.workspace_id
  )

SELECT
  dt,
  workspace_id,
  team_id,
  workspace_created_dttm,
  first_noex_dashboard_created_dttm,
  GREATEST(DATETIME_DIFF(COALESCE(first_noex_dashboard_created_dttm, dt), workspace_created_dttm, MINUTE), 0) as time_to_dashboard_minutes,
  GREATEST(DATETIME_DIFF(COALESCE(first_noex_dashboard_created_dttm, dt), workspace_created_dttm, MINUTE), 0) / 60.0 as time_to_dashboard_hours,
  GREATEST(DATETIME_DIFF(COALESCE(first_noex_dashboard_created_dttm, dt), workspace_created_dttm, MINUTE), 0) / 60.0 / 24.0 as time_to_dashboard_days
FROM
  SSDB
