{{
  config(
    alias='github_daily_metrics_by_repo',
    materialized='table'
  )
}}

WITH github_issues AS (
    SELECT
      *
    FROM
      {{ ref( 'github__issues' ) }}
),

pull_requests AS (
    SELECT
      *
    FROM
      {{ ref( 'github__pull_requests' ) }}
),

issues_opened_per_day AS (
   SELECT
      {{ dbt_utils.date_trunc('day', 'created_at') }} AS day,
      repository,
      COUNT(*) AS number_issues_opened,
      SUM(days_issue_open) AS sum_days_issue_open,
      MAX(days_issue_open) AS longest_days_issue_open
    FROM
      github_issues
    GROUP BY 1, 2
),

issues_closed_per_day AS (
   SELECT
      {{ dbt_utils.date_trunc('day', 'closed_at') }} AS day,
      repository,
      COUNT(*) AS number_issues_closed
    FROM
      github_issues
    WHERE
      closed_at IS NOT NULL
    GROUP BY 1, 2
),

prs_opened_per_day AS (
   SELECT
      {{ dbt_utils.date_trunc('day', 'created_at') }} AS day,
      repository,
      COUNT(*) AS number_prs_opened,
      SUM(days_issue_open) AS sum_days_pr_open,
      MAX(days_issue_open) AS longest_days_pr_open
    FROM
      pull_requests
    GROUP BY 1, 2
),

prs_merged_per_day AS (
   SELECT
      {{ dbt_utils.date_trunc('day', 'merged_at') }} AS day,
      repository,
      COUNT(*) AS number_prs_merged
    FROM
      pull_requests
    WHERE
      merged_at IS NOT NULL
    GROUP BY 1, 2
),

prs_closed_without_merge_per_day AS (
   SELECT
      {{ dbt_utils.date_trunc('day', 'closed_at') }} AS day,
      repository,
      COUNT(*) AS number_prs_closed_without_merge
    FROM pull_requests
    WHERE closed_at IS NOT NULL
      AND merged_at IS NULL
    GROUP BY 1, 2
),

issues_per_day AS (
    SELECT
      COALESCE(issues_opened_per_day.day, issues_closed_per_day.day) AS day,
      COALESCE(issues_opened_per_day.repository, issues_closed_per_day.repository) AS repository,
      number_issues_opened,
      number_issues_closed,
      sum_days_issue_open,
      longest_days_issue_open
    FROM
      issues_opened_per_day
    FULL OUTER JOIN
      issues_closed_per_day
    ON issues_opened_per_day.day = issues_closed_per_day.day
),

prs_per_day AS (
    SELECT
      COALESCE(prs_opened_per_day.day, prs_merged_per_day.day, prs_closed_without_merge_per_day.day) AS day,
      COALESCE(prs_opened_per_day.repository, prs_merged_per_day.repository, prs_closed_without_merge_per_day.repository) AS repository,
      number_prs_opened,
      number_prs_merged,
      number_prs_closed_without_merge,
      sum_days_pr_open,
      longest_days_pr_open
    FROM
      prs_opened_per_day
    FULL OUTER JOIN
      prs_merged_per_day
    ON prs_opened_per_day.day = prs_merged_per_day.day
    FULL OUTER JOIN
      prs_closed_without_merge_per_day
    ON COALESCE(prs_opened_per_day.day, prs_merged_per_day.day) = prs_closed_without_merge_per_day.day
)

SELECT
  COALESCE(issues_per_day.day, prs_per_day.day) AS day,
  COALESCE(issues_per_day.repository, prs_per_day.repository) AS repository,
  COALESCE(number_issues_opened, 0) AS number_issues_opened,
  COALESCE(number_issues_closed, 0) AS number_issues_closed,
  sum_days_issue_open,
  longest_days_issue_open,
  COALESCE(number_prs_opened, 0) AS number_prs_opened,
  COALESCE(number_prs_merged, 0) AS number_prs_merged,
  COALESCE(number_prs_closed_without_merge, 0) AS number_prs_closed_without_merge,
  sum_days_pr_open,
  longest_days_pr_open
FROM
  issues_per_day
FULL OUTER JOIN
  prs_per_day
ON issues_per_day.day = prs_per_day.day
