{{ config(
    alias='time_to_invite',
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'},
) }}

WITH TDS as ( --Team Date Spine
  SELECT
    dt,
    team_id,
    team_created_dttm
  FROM
    {{ ref('team_date_spine') }}
  {% if is_incremental() %}
  WHERE {{ generate_incremental_statement(this) }}
  {% endif %}
),

SMIP AS ( --Source Manager Invites Prep
  SELECT
    team_id,
    MIN(created_dttm) as first_invite_created_dttm,
    MIN(CASE WHEN status = 'ACCEPTED' THEN accepted_at END) as first_invite_accepted_dttm
  FROM
    {{ ref('src_manager_invite') }}
  WHERE
    creator_user_id != accepted_by_user_id
  GROUP BY 1
),

SMI AS (
  SELECT
    TDS.dt,
    TDS.team_id,
    TDS.team_created_dttm,
    FIRST_VALUE(SMIP.first_invite_created_dttm IGNORE NULLS) OVER (PARTITION BY TDS.team_id ORDER BY TDS.dt ASC) as first_invite_created_dttm,
    FIRST_VALUE(SMIP.first_invite_accepted_dttm IGNORE NULLS) OVER (PARTITION BY TDS.team_id ORDER BY TDS.dt ASC) as first_invite_accepted_dttm
  FROM
    TDS
  LEFT JOIN
    SMIP
  ON TDS.dt >= SMIP.first_invite_created_dttm AND TDS.team_id = SMIP.team_id
  )

  SELECT
    dt,
    team_id,
    team_created_dttm,
    first_invite_created_dttm,
    first_invite_accepted_dttm,
    GREATEST(DATETIME_DIFF(COALESCE(first_invite_created_dttm, dt), team_created_dttm, MINUTE), 0) as time_to_invite_sent_minutes,
    GREATEST(DATETIME_DIFF(COALESCE(first_invite_created_dttm, dt), team_created_dttm, MINUTE), 0) / 60.0 as time_to_invite_sent_hours,
    GREATEST(DATETIME_DIFF(COALESCE(first_invite_created_dttm, dt), team_created_dttm, MINUTE), 0) / 60.0 / 24.0 as time_to_invite_sent_days,
    GREATEST(DATETIME_DIFF(COALESCE(first_invite_accepted_dttm, dt), team_created_dttm, MINUTE), 0) as time_to_invite_accepted_minutes,
    GREATEST(DATETIME_DIFF(COALESCE(first_invite_accepted_dttm, dt), team_created_dttm, MINUTE), 0) / 60.0 as time_to_invite_accepted_hours,
    GREATEST(DATETIME_DIFF(COALESCE(first_invite_accepted_dttm, dt), team_created_dttm, MINUTE), 0) / 60.0 / 24.0 as time_to_invite_accepted_days
  FROM
    SMI
