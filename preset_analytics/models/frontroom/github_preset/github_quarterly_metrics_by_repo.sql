{{
  config(
    alias='github_quarterly_metrics_by_repo',
    materialized='table'
  )
}}

WITH daily_metrics AS (
    SELECT
      *
    FROM
      {{ ref( 'github_daily_metrics_by_repo' ) }}
)

SELECT
  {{ dbt_utils.date_trunc('quarter', 'day') }} AS quarter,
  repository,
  SUM(number_issues_opened) AS number_issues_opened,
  SUM(number_issues_closed) AS number_issues_closed,
  SUM(sum_days_issue_open) / SUM(number_issues_opened) AS avg_days_issue_open,
  MAX(longest_days_issue_open) AS longest_days_issue_open,
  SUM(number_prs_opened) AS number_prs_opened,
  SUM(number_prs_merged) AS number_prs_merged,
  SUM(number_prs_closed_without_merge) AS number_prs_closed_without_merge,
  SUM(sum_days_pr_open) / SUM(number_prs_opened) AS avg_days_pr_open,
  MAX(longest_days_pr_open) AS longest_days_pr_open
FROM
  daily_metrics
GROUP BY 1, 2
