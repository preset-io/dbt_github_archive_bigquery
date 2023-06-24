{{ config(
    materialized='incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'dt', 'data_type': 'date'}
) }}
{% set NUM_DAYS_BACKFILL = 30 %}
{% set yesterday = (run_started_at - modules.datetime.timedelta(1)) %}
{% set current_month_YYYYMM = yesterday.strftime("%Y%m") | int %}
{% set yesterday_YYYYMMDD = yesterday.strftime("%Y%m%d") | int %}
SELECT
  DATE(created_at) as dt,
  REPLACE(repo.name, 'incubator-', '') AS community,
  CASE
    WHEN JSON_EXTRACT_SCALAR(payload, '$.action') IS NOT NULL
      THEN CONCAT(`type`, '.', JSON_EXTRACT_SCALAR(payload, '$.action'))
    ELSE `type`
  END AS action,
  `type` AS action_type,
  qry.*,
  CASE
    WHEN
        actor.login LIKE '%[bot]%' OR
        actor.login IN ('asfgit', 'hudi-bot')
      THEN TRUE
    ELSE FALSE END AS is_bot,
  JSON_EXTRACT_SCALAR(payload, '$.action') AS payload_action,
  CAST(JSON_EXTRACT_SCALAR(payload, '$.pull_request.id') AS INT) AS pr_id,
  CAST(JSON_EXTRACT_SCALAR(payload, '$.issue.id') AS INT) AS issue_id,
FROM (
  {% if not is_incremental() %}
    {{ month_range(201501, current_month_YYYYMM) }}
    UNION ALL
    {{ day_range(yesterday.replace(day=1), yesterday) }}
  {% else %}
    {{ day_range(
        (run_started_at - modules.datetime.timedelta(NUM_DAYS_BACKFILL)),
        yesterday
    )}}
  {% endif %}
) AS qry
WHERE repo.name like 'apache/%'
