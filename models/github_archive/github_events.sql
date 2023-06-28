{{ config(materialized='table') }}
SELECT
  A.dt,
  A.repo.name AS repo,
  A.* EXCEPT(dt, payload, repo, actor, org),
  actor.login AS username,
  org.id AS org,
  B.issue_state AS latest_issue_state,
  C.pr_state AS latest_pr_state,
  B.issue_title AS latest_issue_title,
  C.pr_title AS latest_pr_title,
FROM {{ ref("__raw_github_events") }} AS A
LEFT JOIN {{ ref("github_issues") }} AS B ON A.issue_id = B.issue_id
LEFT JOIN {{ ref("github_pull_requests") }} AS C ON A.pr_id = C.pr_id
